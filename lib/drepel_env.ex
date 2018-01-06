require Signal
require MockNode

defmodule Drepel.Env do
    defstruct [ id: 1, sources: [], nodes: %{} ]

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

    def _chooseHandler(parents, deps, initState) do
        if length(parents)==1 do
            case initState do
                %Sentinel{} -> { &Signal.map/5, nil }
                _ -> { &Signal.scan/5, nil }
            end
        else
            if Enum.reduce(deps, true, fn {_source, parents}, acc -> acc && length(parents)==1 end) do
                { &Signal.latest/5, nil }
            else
                { 
                    &Signal.qprop/5, 
                    Enum.reduce(deps, %{}, fn {source, parents}, acc ->
                        sourceBuffs = Enum.reduce(parents, %{}, fn parentId, acc ->
                            Map.put(acc, parentId, :queue.new()) 
                        end)
                        Map.put(acc, source, sourceBuffs)
                    end)
                }
            end
        end
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

    defp stopAll(supervisor, nodeIds) do
        tasks = Enum.map(nodeIds, fn {id, node} ->
            Task.Supervisor.async({Spawner.GenServer, node}, fn ->
                pid = Process.whereis(id)
                Supervisor.terminate_child(supervisor, pid)
            end)
        end)
        Enum.map(tasks, &Task.await(&1))
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:reset, _from, _) do
        { :reply, :ok, %__MODULE__{} }
    end

    def handle_call({:createSource, refreshRate, fct, default, nodeName}, _from, env) do
        id = { String.to_atom("node_#{env.id}"), nodeName }
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
            nodes: Map.put(env.nodes, id, newSource) 
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call({:createEventSource, port, default, nodeName}, _from, env) do
        id = { String.to_atom("node_#{env.id}"), nodeName }
        newSource = %EventSource{
            id: id, 
            port: port,
            default: default,
            dependencies: %{ id => id }
        }
        env = %{ env | 
            id: env.id+1, 
            sources: env.sources ++ [id],
            nodes: Map.put(env.nodes, id, newSource) 
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call({:createSignal, parents, fct, initState, nodeName}, _from, env) do
        id = { String.to_atom("node_#{env.id}"), nodeName }
        dependencies = _computeDepedencies(env, parents)
        {onReceive, buffs} = _chooseHandler(parents, dependencies, initState)
        newSignal = %Signal{ 
            id: id, 
            parents: parents, 
            fct: fct, 
            onReceive: onReceive,
            args: Enum.reduce(parents, %{}, &Map.put(&2, &1, %Sentinel{})),
            dependencies: dependencies,
            buffs: buffs,
            state: initState
        }
        env = Enum.reduce(parents, env, fn (parent, env) ->
            node = Map.get(env.nodes, parent)
            %{ env | nodes: %{ env.nodes | parent => %{ node | children: node.children ++ [id] } } }
        end)
        env = %{ env |  
            id: env.id+1, 
            nodes: Map.put(env.nodes, id, newSignal)
        }
        {:reply, %MockNode{id: id}, env}
    end

    def handle_call(:startNodes, _from, env) do
        # reset stats
        clustNodes = Map.keys(env.nodes) |> Enum.map(&Kernel.elem(&1, 1)) |> Enum.uniq()
        Enum.map(clustNodes, &Drepel.Stats.reset(&1))
        # start signals
        nodes = Map.keys(env.nodes) -- env.sources
        Enum.map(nodes, &Signal.Supervisor.start(Map.get(env.nodes, &1)))
        # start sources
        Enum.map(env.sources, fn id ->
            source = Map.get(env.nodes, id)
            case source do
                %Source{} -> Source.Supervisor.start(source)
                %EventSource{} -> EventSource.Supervisor.start(source)
            end
        end)
        {:reply, :ok, env}
    end

    def handle_call(:stopNodes, _from, env) do
        stopAll(Source.Supervisor, env.sources)
        stopAll(Signal.Supervisor, Map.keys(env.nodes) -- env.sources)
        # get statitics
        clustNodes = Map.keys(env.nodes) |> Enum.map(&Kernel.elem(&1, 1)) |> Enum.uniq()
        stats = Enum.map(clustNodes, &Drepel.Stats.get(&1))
        Enum.map(stats, fn %{latency: %{cnt: cnt, sum: sum, max: max}, works: works} -> 
            avg = cnt>0 && sum/cnt || 0
            work = Enum.map(works, fn {id, %{cnt: cnt, sum: sum}} -> 
                cnt>0 && sum/cnt || 0
            end) |> Enum.join(" ")
            
            IO.puts "#{work} #{max} #{cnt} #{sum} #{avg}"
        end)
        #Enum.map(stats, fn %{works: works} -> 
        #    Enum.map(works, fn {id, %{cnt: cnt, sum: sum}} -> 
        #        avg = cnt>0 && sum/cnt || 0
        #        IO.puts "#{inspect id} #{cnt} #{sum} #{avg}"
        #    end)
        #end)
        {:reply, :ok, env}
    end

end