require Signal
require MockNode
require Logger

defmodule Drepel.Env do
    defstruct [ id: 1, sources: [], nodes: %{}, clustNodes: [], 
    repFactor: 1, balancingInterval: 10000, chckptInterval: 1000, 
    routing: %{}, eSources: [], timer: nil, running: false ]

    use GenServer

    def computeDepedencies(env, parents) do
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

    def stopAll(env) do
        supervisors = [Source.Supervisor, Signal.Supervisor]
        Enum.map(supervisors, &stopAllChildren(env.clustNodes, &1))
    end

    def stopAllChildren(clustNodes, supervisor) do
        req = {:stopAllChildren, supervisor}
        Utils.multi_call(clustNodes -- [node()], __MODULE__, req)
        handle_call(req, nil, nil)
    end

    def listSinks(env) do
        Enum.reduce(env.nodes, [], fn {id, node}, acc -> 
            length(node.children)>0 && acc || acc ++ [id]
        end)
    end

    def computeNewRouting(env, nodesDown) do
        oldClustNodes = Map.values(env.routing) |> Enum.uniq()
        Enum.reduce(env.routing, %{}, fn {id, node}, acc ->
            if Enum.member?(nodesDown, node) do
                repNodes = computeRepNodes(env, oldClustNodes, node)
                newNode = Enum.at(repNodes -- nodesDown, 0)
                Map.put(acc, id, newNode)
            else
                Map.put(acc, id, node)
            end
        end)
    end

    def computeRepNodes(env, clustNodes, node) do
        if (env.chckptInterval>0) do
            nbNodes = length(clustNodes)
            pos = Enum.find_index(clustNodes, fn clustNode -> clustNode==node end)
            Enum.map(pos..pos+env.repFactor, fn i -> Enum.at(clustNodes, rem(i, nbNodes)) end)
            |> Enum.uniq()
        else
            []
        end
    end

    def resetStats(routing) do
        cNodesToGNodes = Map.to_list(routing) |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
        Enum.map(cNodesToGNodes, fn {cNode, gNodes} -> 
            Sampler.reset(cNode, gNodes) 
        end)
    end
    
    def _restore(env, chckptId) do
        leader = Enum.at(env.clustNodes, 0)
        # replicate env
        env.clustNodes -- [node()]
        |> Drepel.Env.replicate(env)
        # reset stats
        resetStats(env.routing)
        # reset checkpointing
        sourcesRouting = Map.take(env.routing, env.sources)
        Checkpoint.reset(leader, listSinks(env), env.clustNodes, sourcesRouting, env.chckptInterval)
        # 
        updateRepNodes(env)
        # restart all signals
        restartSignals(env, chckptId, env.clustNodes)
        # restart all sources
        restartSources(env, chckptId)
        # restart stats sampling
        Sampler.start(env.clustNodes)
        # restart checkpointing
        Checkpoint.start()
        # reset balancer
        Balancer.reset(env.clustNodes, env.routing, env.balancingInterval)
        Logger.info "system restarted"
    end

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

    def setBalancingInterval(interval) do
        GenServer.call(__MODULE__, {:setBalancingInterval, interval})
    end

    def createBSource(refreshRate, fct, default, opts) do
        node = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createBSource, refreshRate, fct, default, node})
    end

    def createESource(name, default, opts) do
        node = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createESource, name, default, node})
    end

    def createSignal(parents, fct, opts) do
        node = :proplists.get_value(:node, opts, node())
        GenServer.call(__MODULE__, {:createSignal, parents, fct, %Sentinel{}, node})
    end

    def createStatedNode(parents, fct, initState, opts) do
        node = :proplists.get_value(:node, opts, node())    
        GenServer.call(__MODULE__, {:createSignal, parents, fct, initState, node})
    end

    def move(to, ids) do
        GenServer.cast(__MODULE__, {:move, to, ids})
    end

    def startNodes(duration) do
        GenServer.call(__MODULE__, {:startNodes, duration})
    end

    def startSignals(env) do
        req = {:startSignals, env.clustNodes, env.routing}
        Utils.multi_call(env.clustNodes -- [node()], __MODULE__, req)
        handle_call(req, nil, env)
    end

    def startSources(env) do
        req = {:startSources, env.routing}
        Utils.multi_call(env.clustNodes -- [node()], __MODULE__, req)
        handle_call(req, nil, env)
    end

    def restartSignals(env, chckptId, clustNodes) do
        req = {:restartSignals, clustNodes, env.routing, chckptId}
        Utils.multi_call(env.clustNodes -- [node()], __MODULE__, req)
        handle_call(req, nil, env)
    end

    def restartSources(env, chckptId) do
        req = {:restartSources, env.routing, chckptId}
        Utils.multi_call(env.clustNodes -- [node()], __MODULE__, req)
        handle_call(req, nil, env)
    end

    def stopNodes do
        GenServer.call(__MODULE__, :stopNodes)
    end

    def replicate(nodes, env) do
        Utils.multi_call(nodes, __MODULE__, {:replicate, env})
    end

    def restore(chckptId, clustNodes, nodesDown) do
        GenServer.call(__MODULE__, {:restore, chckptId, clustNodes, nodesDown})
    end

    def test do
        GenServer.call(__MODULE__, :test)
    end

    def join(nodes) do
        GenServer.call(__MODULE__, {:join, nodes})
    end

    def discover(node) do
        try do
            GenServer.call({__MODULE__, node}, {:discover, node()})
        catch
            :exit, _msg -> :error
        end
    end

    def addClustNode(nodes, clustNode) do
        Utils.multi_call(nodes, __MODULE__,  {:addClustNode, clustNode})
    end

    def updateRepNodes(env) do
        GenServer.multi_call(env.clustNodes -- [node()], __MODULE__, :updateRepNodes )
        handle_call(:updateRepNodes , nil, env)
    end

    

    # Server API

    def init(:ok) do
        :mnesia.delete_schema([node()])
        { :ok, %__MODULE__{} }
    end

    def handle_call({:stopAllChildren, supervisor}, _from, env) do
        Supervisor.which_children(supervisor)
        |> Enum.map(&Supervisor.terminate_child(supervisor, elem(&1, 1)))
        {:reply, :ok, env}
    end

    def handle_call({:addClustNode, clustNode}, _from, env) do
        clustNodes = env.clustNodes ++ [clustNode]
        ClusterSupervisor.monitor(clustNodes)
        { :reply, :ok, %{ env | clustNodes: clustNodes } }
    end

    def handle_call(:updateRepNodes, _from, env) do
        repNodes = computeRepNodes(env, env.clustNodes, node()) 
        ids = Enum.filter(env.routing, &(elem(&1,1)==node()))
        |> Enum.map(&(elem(&1,0)))
        Store.setRepNodes(repNodes, ids)
        { :reply, :ok, env}
    end

    def handle_call({:discover, clustNode}, _from, env) do
        leader = Enum.at(env.clustNodes, 0)
        :mnesia.change_config(:extra_db_nodes, [clustNode])
        Balancer.join(leader, clustNode)
        ClusterSupervisor.monitor(env.clustNodes ++ [clustNode])
        env.clustNodes -- [node()]
        |> addClustNode(clustNode)
        newEnv = %{ env | clustNodes: env.clustNodes ++ [clustNode] }
        { :reply, newEnv, newEnv }
    end

    def handle_call({:join, nodes}, _from, env) do
        Sampler.reset(node(), [])
        Sampler.start()
        :mnesia.start()
        res = Enum.reduce_while(nodes, nil, fn node, _ ->
            newEnv = Drepel.Env.discover(node)
            case newEnv do
                %__MODULE__{} -> 
                    ClusterSupervisor.monitor(newEnv.clustNodes)
                    { :halt, newEnv }
                :error -> { :cont, :error }
            end
        end)
        case res do
            :error -> { :reply, :error, env }
            _ -> { :reply, :ok, res}
        end
    end

    def handle_call(:reset, _from, _) do
        { :reply, :ok, %__MODULE__{} }
    end

    def handle_call({:setCheckpointInterval, interval}, _from, env) do
        { :reply, :ok, %{ env | chckptInterval: interval }  }
    end

    def handle_call({:setBalancingInterval, interval}, _from, env) do
        { :reply, :ok, %{ env | balancingInterval: interval} }
    end

    def handle_call({:replicate, env}, _from, _env) do
        { :reply, :ok, env }
    end

    def handle_call({:createBSource, refreshRate, fct, default, node}, _from, env) do
        if !env.running do
            id = String.to_atom("node_#{env.id}")
            newSource = %BSource{
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
                routing: Map.put(env.routing, id, node)
            }
            {:reply, %MockNode{id: id}, env}
        else
            {:reply, {:err, :running}, env}
        end
    end

    def handle_call({:createESource, name, default, node}, _from, env) do
        if !env.running do
            id = String.to_atom("node_#{env.id}")
            newSource = %ESource{
                id: id, 
                name: name,
                default: default,
                dependencies: %{ id => id }
            }
            env = %{ env | 
                id: env.id+1, 
                sources: env.sources ++ [id],
                nodes: Map.put(env.nodes, id, newSource),
                routing: Map.put(env.routing, id, node),
                eSources: env.eSources ++ [id]
            }
            {:reply, %MockNode{id: id}, env}
        else
            {:reply, {:err, :running}, env}
        end
    end

    def handle_call({:createSignal, parents, fct, initState, node}, _from, env) do
        if !env.running do
            id = String.to_atom("node_#{env.id}")
            dependencies = computeDepedencies(env, parents)
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
                routing: Map.put(env.routing, id, node)
            }
            {:reply, %MockNode{id: id}, env}
        else
            {:reply, {:err, :running}, env}
        end
    end

    def handle_call({:startSources, routing}, _from, env) do
        env.sources 
        |> Enum.filter(&(Map.get(routing, &1)==node()))
        |> Enum.map(fn id ->
            source = Map.get(env.nodes, id)
            Source.Supervisor.start_child(%{ source | 
                routing: routing, 
            })
        end)
        {:reply, :ok, env}
    end

    def handle_call({:startSignals, clustNodes, routing}, _from, env) do
        Map.keys(env.nodes) -- env.sources 
        |> Enum.filter(&(Map.get(routing, &1)==node()))
        |> Enum.map(fn id -> 
            signal = Map.get(env.nodes, id)
            Signal.Supervisor.start_child(%{ signal | 
                routing: routing, 
                leader: Enum.at(clustNodes, 0)
            })
        end)
        {:reply, :ok, env}
    end

    def handle_call({:startNodes, duration}, _from, env) do
        Logger.info "system started"
        env = %{ env | 
            clustNodes: Map.values(env.routing) |> Enum.uniq(),
            running: true
        }
        leader = Enum.at(env.clustNodes, 0)
        if length(env.clustNodes)<env.repFactor do
            raise "replication factor is too high"
        end
        Enum.filter(env.clustNodes, &(&1!=node()))
        |> Drepel.Env.replicate(env)
        # reset stats
        resetStats(env.routing)
        # reset stores
        Store.reset(env.clustNodes)
        # reset checkpointing
        sourcesRouting = Map.take(env.routing, env.sources)
        Checkpoint.reset(leader, listSinks(env), env.clustNodes, sourcesRouting, env.chckptInterval)
        # set up nodes monitoring
        ClusterSupervisor.monitor(env.clustNodes)
        # init store
        Store.start(env.clustNodes)
        Store.createTables(env)
        # start signals
        startSignals(env)
        # start sources
        startSources(env)
        # TODO
        MyStuff.reset(Enum.map(env.eSources, fn id ->
            name = Map.get(env.nodes, id).name
            { Map.get(env.routing, id), {id, name} }
        end))
        # start sampler
        Sampler.start(env.clustNodes)
        # start checkpointing
        Checkpoint.start(leader)
        # reset balancer
        Balancer.reset(env.clustNodes, env.routing, env.balancingInterval)
        timer = case duration do
            :inf -> nil
            _ -> Process.send_after(self(), :stopNodes, duration)
        end
        { :reply, :ok, %{ env | timer: timer } }
    end

    def handle_call(:stopNodes, _from, env) do
        leader = Enum.at(env.clustNodes, 0)
        # stop balancer
        Balancer.stop(leader)
        # stop checkpoint
        Checkpoint.stop(leader)
        # stop stats
        Sampler.stop(env.clustNodes)
        # stop nodes
        stopAll(env)
        Utils.cancelTimer(env.timer)
        Logger.info "system stopped"
        {:reply, :ok, %{ env | timer: nil, running: false } }
    end

    def handle_call({:restore, chckptId, clustNodes, nodesDown}, _from, env) do
        newRouting = computeNewRouting(env, nodesDown)
        env = %{ env | clustNodes: clustNodes, routing: newRouting }
        _restore(env, chckptId)
        { :reply, :ok, env }
    end

    def handle_call({:restartSignals, clustNodes, routing, chckptId}, _from, env) do
        leader = Enum.at(clustNodes, 0)
        Map.keys(env.nodes) -- env.sources
        |> Enum.each(fn id ->
            aSignal = Store.get(id, chckptId)
            Signal.Supervisor.start_child(%{ aSignal | 
                routing: routing, 
                leader: leader
            })
        end)
        { :reply, :ok, env }
    end

    def handle_call({:restartSources, routing, chckptId}, _from, env) do
        env.sources 
        |> Enum.each(fn id ->
            aSource = Map.get(env.nodes, id)
            messages = Store.getMessages(id, chckptId)
            Source.Supervisor.start_child(
                %{ aSource | 
                    routing: routing,
                }, 
                messages
            )
        end)
        { :reply, :ok, env }
    end

    def handle_cast({:move, to, ids}, env) do
        # stop all sources/signals
        stopAll(env)
        # compute new routing
        newRouting = Enum.reduce(ids, env.routing, &Map.put(&2, &1, to))
        # restore last checkpoint
        chckptId = Checkpoint.lastCompleted()
        env = %{ env | routing: newRouting}
        _restore(env, chckptId)
        { :noreply, env }
    end

    def handle_info(:stopNodes, env) do 
        {:reply, :ok, env} = handle_call(:stopNodes, nil, env)
        { :noreply, env }
    end

end