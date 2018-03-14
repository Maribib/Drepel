require Signal
require MockNode
require Logger

defmodule Drepel.Env do
    defstruct [ id: 1, sources: [], nodes: %{}, clustNodes: [], 
    repFactor: 1, chckptInterval: 1000, routing: %{} ]

    use GenServer
    
    # Client API

    def start_link(_opts) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset do
        GenServer.call(__MODULE__, :reset)
    end

    def setCheckpointInterval(interval) do
        GenServer.call(__MODULE__, {:setCheckpointInterval, interval})
    end

    def createSource(refreshRate, fct, default, opts) do
        nodeName = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createSource, refreshRate, fct, default, nodeName})
    end

    def createEventSource(port, default, opts) do
        nodeName = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createEventSource, port, default, nodeName})
    end

    def createSignal(parents, fct, opts) do
        nodeName = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createSignal, parents, fct, %Sentinel{}, nodeName})
    end

    def createStatedNode(parents, fct, initState, opts) do
        nodeName = :proplists.get_value(:node, opts, node())    
        GenServer.call(__MODULE__, {:createSignal, parents, fct, initState, nodeName})
    end

    def balance(from, to, ids) do
        GenServer.cast(__MODULE__, {:balance, from, to, ids})
    end

    def startNodes do
        GenServer.call(__MODULE__, :startNodes)
    end

    def stopNodes do
        GenServer.call(__MODULE__, :stopNodes)
    end

    def _computeDepedencies(env, parents) do
        Enum.reduce(parents, %{}, fn parentId, deps ->
            parentDeps = Map.get(env.nodes, parentId).dependencies
            Enum.reduce(parentDeps, deps, fn {source, _}, deps ->
                if Map.has_key?(deps, source) do
                    %{ deps | source => Map.get(deps, source) ++ [parentId] }
                else
                    Map.put(deps, source, [parentId])
                end
            end)
        end)
    end

    defp stopAll(supervisor, nodeIds, routing) do
        tasks = Enum.map(nodeIds, fn id ->
            node = Map.get(routing, id)
            Task.Supervisor.async({Task.Spawner, node}, fn ->
                pid = Process.whereis(id)
                Supervisor.terminate_child(supervisor, pid)
            end)
        end)
        Enum.map(tasks, &Task.await(&1))
    end

    def listSinks(env) do
        Enum.reduce(env.nodes, [], fn {id, node}, acc -> 
            length(node.children)>0 && acc || acc ++ [id]
        end)
    end

    def replicate(nodeName, env) do
        GenServer.call({__MODULE__, nodeName}, {:replicate, env})
    end

    def restore(chckptId, clustNodes, nodesDown) do
        GenServer.call(__MODULE__, {:restore, chckptId, clustNodes, nodesDown})
    end

    def computeNewRouting(env, nodesDown) do
        oldClustNodes = Map.values(env.routing) |> Enum.uniq()
        Enum.reduce(env.routing, %{}, fn {id, node}, acc ->
            if Enum.member?(nodesDown, node) do
                repNodes = computeRepNodes(oldClustNodes, env.repFactor, node)
                newNode = Enum.reduce_while(repNodes, nil, fn repNode, acc ->
                    if Enum.member?(nodesDown, repNode) do
                        { :cont, acc }
                    else
                        { :halt, repNode }
                    end
                end)
                Map.put(acc, id, newNode)
            else
                Map.put(acc, id, node)
            end
        end)
    end

    def restartSignals(env, chckptId, nodesDown, clustNodes) do
        leader = Enum.at(clustNodes, 0)
        Enum.map(Map.keys(env.nodes) -- env.sources, fn id ->
            node = Map.get(env.routing, id)
            node = if Enum.member?(nodesDown, node) do
                Map.get(env.routing, id)
            else
                node
            end
            Signal.Supervisor.restart( 
                node,
                id, 
                chckptId,
                env.routing,
                computeRepNodes(clustNodes, env.repFactor, node),
                leader 
            )
        end)
    end

    def restartSources(env, chckptId, nodesDown, clustNodes) do
        Enum.map(env.sources, fn id ->
            node = Map.get(env.routing, id)
            node = if Enum.member?(nodesDown, node) do
                Map.get(env.routing, id)
            else
                node
            end
            aSource = Map.get(env.nodes, id)
            sourceSupervisorCls = Module.concat(aSource.__struct__, Supervisor)
            sourceSupervisorCls.restart(
                %{ aSource | 
                    routing: env.routing,
                    repNodes: computeRepNodes(clustNodes, env.repFactor, node)
                }, 
                node, 
                id, 
                chckptId
            )
        end)
    end

    def computeRepNodes(clustNodes, repFactor, node) do
        nbNodes = length(clustNodes)
        pos = Enum.find_index(clustNodes, fn clustNode -> clustNode==node end)
        Enum.map(pos..pos+repFactor, fn i -> Enum.at(clustNodes, rem(i, nbNodes)) end)
        |> Enum.uniq()
    end

    def resetStats(routing) do
        clustNodesToGraphNode = Map.to_list(routing) |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
        Enum.map(clustNodesToGraphNode, fn {clustNode, graphNodes} -> 
            Drepel.Stats.reset(clustNode, graphNodes) 
        end)
    end

    def test do
        GenServer.call(__MODULE__, :test)
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:reset, _from, _) do
        { :reply, :ok, %__MODULE__{} }
    end

    def handle_call({:setCheckpointInterval, interval}, _from, env) do
        { :reply, :ok, %{ env | chckptInterval: interval }  }
    end

    def handle_call({:replicate, env}, _from, _env) do
        { :reply, :ok, env }
    end

    def handle_call({:createSource, refreshRate, fct, default, nodeName}, _from, env) do
        id = String.to_atom("node_#{env.id}")
        newSource = %Source{
            id: id, 
            refreshRate: refreshRate, 
            fct: fct, 
            default: default,
            dependencies: %{ id => id }
        }
        env = %{ env | 
            id: env.id+1, 
            sources: env.sources ++ [id],
            nodes: Map.put(env.nodes, id, newSource),
            routing: Map.put(env.routing, id, nodeName)
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call({:createEventSource, port, default, nodeName}, _from, env) do
        id = String.to_atom("node_#{env.id}")
        newSource = %EventSource{
            id: id, 
            port: port,
            default: default,
            dependencies: %{ id => id }
        }
        env = %{ env | 
            id: env.id+1, 
            sources: env.sources ++ [id],
            nodes: Map.put(env.nodes, id, newSource),
            routing: Map.put(env.routing, id, nodeName)
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call({:createSignal, parents, fct, initState, nodeName}, _from, env) do
        id = String.to_atom("node_#{env.id}")
        dependencies = _computeDepedencies(env, parents)
        newSignal = %Signal{ 
            id: id, 
            parents: parents, 
            fct: fct, 
            args: Enum.reduce(parents, %{}, &Map.put(&2, &1, %Sentinel{})),
            dependencies: dependencies,
            state: initState
        }
        env = Enum.reduce(parents, env, fn (parent, env) ->
            node = Map.get(env.nodes, parent)
            %{ env | nodes: %{ env.nodes | parent => %{ node | children: node.children ++ [id] } } }
        end)
        env = %{ env |  
            id: env.id+1, 
            nodes: Map.put(env.nodes, id, newSignal),
            routing: Map.put(env.routing, id, nodeName)
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call(:startNodes, _from, env) do
        clustNodes = Map.values(env.routing) |> Enum.uniq()
        leader = Enum.at(clustNodes, 0)
        if length(clustNodes)<env.repFactor do
            raise "replication factor is too high"
        end
        Enum.filter(clustNodes, &(&1!=node()))
        |> Enum.map(&Drepel.Env.replicate(&1, env))
        # reset stats
        resetStats(env.routing)
        # reset stores
        Enum.map(clustNodes, &Store.reset(&1))
        # reset checkpointing
        sourcesRouting = Map.take(env.routing, env.sources)
        Checkpoint.reset(leader, listSinks(env), clustNodes, sourcesRouting, env.chckptInterval)
        # set up nodes monitoring
        Enum.map(clustNodes, &Node.Supervisor.monitor(&1, clustNodes))
        # start signals
        Map.keys(env.nodes) -- env.sources
        |> Enum.map(fn id ->
            signal = Map.get(env.nodes, id)
            node = Map.get(env.routing, id)
            repNodes = computeRepNodes(clustNodes, env.repFactor, node)
            Signal.Supervisor.start(%{ signal | 
                routing: env.routing, 
                repNodes: repNodes,
                leader: Enum.at(clustNodes, 0)
            })
        end)
        # start sources
        Enum.map(env.sources, fn id ->
            source = Map.get(env.nodes, id)
            node = Map.get(env.routing, id)
            sourceSupervisorCls = Module.concat(source.__struct__, Supervisor)
            sourceSupervisorCls.start(%{ source | 
                routing: env.routing, 
                repNodes: computeRepNodes(clustNodes, env.repFactor, node)
            })
        end)
        Enum.map(clustNodes, &Drepel.Stats.startSampling(&1))
        Checkpoint.startCheckpointing(leader)
        Balancer.reset(leader, clustNodes, env.routing)
        Logger.info "system started"
        {:reply, :ok, %{ env | clustNodes: clustNodes } }
    end

    def handle_call(:stopNodes, _from, env) do
        # stop stats
        Enum.map(env.clustNodes, &Drepel.Stats.stopSampling(&1))
        # stop nodes
        stopAll(Source.Supervisor, env.sources, env.routing)
        stopAll(Signal.Supervisor, Map.keys(env.nodes) -- env.sources, env.routing)
        # get statitics
        #stats = Enum.map(env.clustNodes, &Drepel.Stats.get(&1))
        #Enum.map(stats, fn %{latency: %{cnt: cnt, sum: sum, max: max}, works: works} -> 
        #    avg = cnt>0 && sum/cnt || 0
        #    work = Enum.map(works, fn {_id, %{cnt: cnt, sum: sum}} -> 
        #        cnt>0 && sum/cnt || 0
        #    end) |> Enum.join(" ")
        #    
        #    IO.puts "#{work} #{max} #{cnt} #{sum} #{avg}"
        #end)
        #Enum.map(stats, fn %{msgQ: msgQ} -> 
        #    Enum.map(msgQ, fn {signal, samples} -> 
        #        IO.puts "#{signal} #{Enum.join(samples, " ")}"
        #    end)
        #end)
        #Enum.map(stats, fn %{works: works} -> 
        #    Enum.map(works, fn {id, %{cnt: cnt, sum: sum}} -> 
        #        avg = cnt>0 && sum/cnt || 0
        #        IO.puts "#{inspect id} #{cnt} #{sum} #{avg}"
        #    end)
        #end)
        {:reply, :ok, env}
    end

    def handle_call({:restore, chckptId, clustNodes, nodesDown}, _from, env) do
        leader = Enum.at(clustNodes, 0)
        # compute new routing table
        newRouting = computeNewRouting(env, nodesDown)
        env = %{ env | routing: newRouting }
        # replicate env
        Enum.filter(clustNodes, &(&1!=node()))
        |> Enum.map(&Drepel.Env.replicate(&1, env))
        # reset stats
        resetStats(newRouting)
        # reset checkpointing
        sourcesRouting = Map.take(newRouting, env.sources)
        Checkpoint.reset(leader, listSinks(env), clustNodes, sourcesRouting, env.chckptInterval)
        # restart all signals
        restartSignals(env, chckptId, nodesDown, clustNodes)
        # restart all sources
        restartSources(env, chckptId, nodesDown, clustNodes)
        Enum.map(clustNodes, &Drepel.Stats.startSampling(&1))
        Checkpoint.startCheckpointing()
        Balancer.reset(clustNodes, newRouting)
        Logger.info "system restarted"
        { :reply, :ok, env}
    end

    def computeAncestors(env, ids) do
        Enum.reduce(ids, MapSet.new(ids), fn id, acc ->
            if Enum.member?(env.sources, id) do
                MapSet.put(acc, id)
            else
                MapSet.union(acc, computeAncestors(env, env.nodes[id].parents))
                |> MapSet.put(id)
            end
        end)
    end

    def handle_cast({:balance, from, to, ids}, env) do
        leader = Enum.at(env.clustNodes, 0) 
        newRouting = Enum.reduce(ids, env.routing, &Map.put(&2, &1, to))
        ancestors = computeAncestors(env, ids) |> MapSet.to_list()

        # stop stats
        [from, to] 
        |> Enum.map(&Drepel.Stats.stopSampling(&1))

        # reset stats
        Map.to_list(newRouting) 
        |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
        |> Map.take([from, to])
        |> Enum.map(fn {clustNode, graphNodes} -> 
            Drepel.Stats.reset(clustNode, graphNodes) 
        end)

        sourcesAncestors = ancestors -- (Map.keys(env.nodes) -- env.sources)
        signalsAncestors = ancestors -- env.sources
        # stop sources ancestors
        stopAll(Source.Supervisor, sourcesAncestors, env.routing)
        # stop signals ancestors
        stopAll(Signal.Supervisor, signalsAncestors, env.routing)
        
        chckptId = Checkpoint.lastCompleted()
        # restart all signals
        Enum.map(signalsAncestors, fn id ->
            node = newRouting[id]
            Signal.Supervisor.restart( 
                node,
                id, 
                chckptId,
                newRouting,
                computeRepNodes(env.clustNodes, env.repFactor, node),
                leader
            )
        end)
        # restart all sources
        Enum.map(sourcesAncestors, fn id ->
            node = newRouting[id]
            aSource = Map.get(env.nodes, id)
            sourceSupervisorCls = Module.concat(aSource.__struct__, Supervisor)
            sourceSupervisorCls.restart(
                %{ aSource | 
                    routing: newRouting,
                    repNodes: computeRepNodes(env.clustNodes, env.repFactor, node)
                }, 
                node, 
                id, 
                chckptId
            )
        end)
        # start stats
        [from, to]
        |> Enum.map(&Drepel.Stats.startSampling(&1))
        # start checkpointing
        Checkpoint.startCheckpointing()
        # reset balancer
        Balancer.reset(env.clustNodes, newRouting)
        # replicate env
        env = %{ env | routing: newRouting }
        Enum.filter(env.clustNodes, &(&1!=node()))
        |> Enum.map(&Drepel.Env.replicate(&1, env))
        { :noreply, env }
    end

end