require Signal
require MockNode

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

    def replicate(nodeName, sources, nodes, routing, repFactor, chckptInterval) do
        GenServer.call(
            {__MODULE__, nodeName}, 
            {
                :replicate, 
                sources, 
                nodes, 
                routing, 
                repFactor, 
                chckptInterval
            }
        )
    end

    def restore(chckptId, clustNodes, nodesDown) do
        GenServer.call(__MODULE__, {:restore, chckptId, clustNodes, nodesDown})
    end

    def computeNewRouting(env, nodesDown) do
        oldClustNodes = Map.values(env.routing) |> Enum.uniq()
        Enum.reduce(env.routing, %{}, fn {id, node}, acc ->
            if Enum.member?(nodesDown, node) do
                repNodes = Store.computeRepNodes(oldClustNodes, env.repFactor, node)
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

    def restartSignals(env, chckptId, nodesDown, clustNodes, newRouting) do
        Enum.map(Map.keys(env.nodes) -- env.sources, fn id ->
            aNode = Map.get(env.routing, id)
            aNode = if Enum.member?(nodesDown, aNode) do
                Map.get(newRouting, id)
            else
                aNode
            end
            Signal.Supervisor.restart(aNode, id, chckptId, newRouting, clustNodes, env.repFactor)
        end)
    end

    def restartSources(env, chckptId, nodesDown, clustNodes, newRouting) do
        Enum.map(env.sources, fn id ->
            aNode = Map.get(env.routing, id)
            aNode = if Enum.member?(nodesDown, aNode) do
                Map.get(newRouting, id)
            else
                aNode
            end
            source = Map.get(env.nodes, id)
            sourceCls = Module.concat(source.__struct__, Supervisor)
            sourceCls.restart(source, aNode, id, chckptId, newRouting, clustNodes, env.repFactor, env.chckptInterval)
        end)
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:reset, _from, _) do
        { :reply, :ok, %__MODULE__{} }
    end

    def handle_call({:replicate, sources, nodes, routing, repFactor, chckptInterval}, _from, env) do
        { :reply, :ok, %{ env | 
            sources: sources, 
            nodes: nodes,
            routing: routing,
            repFactor: repFactor,
            chckptInterval: chckptInterval
        } }
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
        if length(clustNodes)<env.repFactor do
            IO.puts "replication factor is too high"
        end
        Enum.filter(clustNodes, &(&1!=node()))
        |> Enum.map(&Drepel.Env.replicate(&1, env.sources, env.nodes, env.routing, env.repFactor, env.chckptInterval))
        # reset stats
        clustNodesToSignals = Map.to_list(env.routing) |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
        Enum.map(clustNodesToSignals, fn {clustNode, signals} -> 
            Drepel.Stats.reset(clustNode, signals) 
        end)
        # reset stores
        Enum.map(clustNodes, &Store.reset(&1))
        # reset checkpointing
        leader = Enum.at(clustNodes, 0)
        Checkpoint.reset(leader, listSinks(env), clustNodes)
        # nodes monitoring set up
        Enum.map(clustNodes, &Node.Supervisor.monitor(&1, clustNodes))
        # start signals
        nodes = Map.keys(env.nodes) -- env.sources
        Enum.map(nodes, &Signal.Supervisor.start(%{ Map.get(env.nodes, &1) | routing: env.routing }, clustNodes, env.repFactor))
        # start sources
        Enum.map(env.sources, fn id ->
            source = Map.get(env.nodes, id)
            case source do
                %Source{} -> 
                    Source.Supervisor.start(%{ source | routing: env.routing }, clustNodes, env.repFactor, env.chckptInterval)
                %EventSource{} -> 
                    EventSource.Supervisor.start(%{ source | routing: env.routing }, clustNodes, env.repFactor, env.chckptInterval)
            end
        end)
        Enum.map(Map.keys(clustNodesToSignals), &Drepel.Stats.startSampling(&1))
        {:reply, :ok, %{ env | clustNodes: clustNodes } }
    end

    def handle_call(:stopNodes, _from, env) do
        # stop stats
        Enum.map(env.clustNodes, &Drepel.Stats.stopSampling(&1))
        # stop nodes
        stopAll(Source.Supervisor, env.sources, env.routing)
        stopAll(Signal.Supervisor, Map.keys(env.nodes) -- env.sources, env.routing)
        # get statitics
        stats = Enum.map(env.clustNodes, &Drepel.Stats.get(&1))
        Enum.map(stats, fn %{latency: %{cnt: cnt, sum: sum, max: max}, works: works} -> 
            avg = cnt>0 && sum/cnt || 0
            work = Enum.map(works, fn {_id, %{cnt: cnt, sum: sum}} -> 
                cnt>0 && sum/cnt || 0
            end) |> Enum.join(" ")
            
            IO.puts "#{work} #{max} #{cnt} #{sum} #{avg}"
        end)
        Enum.map(stats, fn %{msgQ: msgQ} -> 
            Enum.map(msgQ, fn {signal, samples} -> 
                IO.puts "#{signal} #{Enum.join(samples, " ")}"
            end)
        end)
        #Enum.map(stats, fn %{works: works} -> 
        #    Enum.map(works, fn {id, %{cnt: cnt, sum: sum}} -> 
        #        avg = cnt>0 && sum/cnt || 0
        #        IO.puts "#{inspect id} #{cnt} #{sum} #{avg}"
        #    end)
        #end)
        {:reply, :ok, env}
    end

    def handle_call({:restore, chckptId, clustNodes, nodesDown}, _from, env) do
        # compute new routing table
        newRouting = computeNewRouting(env, nodesDown)
        # reset checkpointing
        leader = Enum.at(clustNodes, 0)
        Checkpoint.reset(leader, listSinks(env), clustNodes)
        # restart all signals
        restartSignals(env, chckptId, nodesDown, clustNodes, newRouting)
        # restart all sources
        restartSources(env, chckptId, nodesDown, clustNodes, newRouting)
        { :reply, :ok, env}
    end

end