require DNode
require MockDNode

defmodule Drepel.Env do
    defstruct [ id: 1, children: [], nodes: %{}, running: false,
    toRun: [], orderSensitive: false ]

    def new(opts) do
        Agent.start_link(fn -> %__MODULE__{ 
            orderSensitive: :proplists.get_value(:orderSensitive, opts, false)
        } end, name: __MODULE__)
    end

    def get do
        Agent.get(__MODULE__, &(&1))
    end

    def get(key) do
        Agent.get(__MODULE__, &Map.get(&1, key))
    end

    def getNode(key) do
        Agent.get(__MODULE__, &Map.get(&1.nodes, key))
    end

    def _addChild(parent, env) do
        id = String.to_atom("dnode_#{env.id}")
        if parent != :dnode_0 do
            node = Map.get(env.nodes, parent)
            %{ env | nodes: %{ env.nodes | parent => %{ node | children: node.children ++ [id] } } }
        else
            %{ env | children: env.children ++ [id] }
         end
    end

    def _addNode(env, parents, runFct, initState \\ &nilState/1, isSink \\ false, reorderer \\ nil) do
        id = String.to_atom("dnode_#{env.id}")
        newDNode = %DNode{ 
            id: id, parents: parents, runFct: runFct, 
            initState: initState, isSink: isSink, reorderer: reorderer
        }
        env = %{ env | toRun: env.toRun ++ [id] }
        env = case reorderer do
            nil -> Enum.reduce(parents, env, &_addChild/2 )
            _ -> _addChild(reorderer, env)
        end
        { 
            %MockDNode{id: id}, 
            %{ env |  
                id: env.id+1, 
                nodes: Map.put(env.nodes, id, newDNode)
            }
        }
    end

    def startAllMidNodes(env, nodes) do
        Enum.map(nodes, &DNode.Supervisor.start(Map.get(env.nodes, &1)))
        nodes
    end

    def startAllSources(env, sources) do
        Enum.map(sources, &DNode.Supervisor.start(Map.get(env.nodes, &1)))
        Enum.map(sources, &DNode.runSource(&1))
    end

    def startAllNodes do
        Agent.get_and_update(__MODULE__, fn env ->
            nodes = startAllMidNodes(env, env.toRun -- env.children)
            startAllSources(env, env.children -- ( env.children -- env.toRun ))
            { nodes, %{ env | toRun: [] } }
        end)
    end

    def addTmpChild(child, parent) do
        Agent.update(__MODULE__, fn env ->
            aDNode = Map.get(env.nodes, child)
            %{ env | nodes: %{ env.nodes | child => %{ aDNode | children: aDNode.children ++ [parent] } } }
        end)
    end

    def getAll(env, id, extractor) do
        nod = Map.get(env.nodes, id)
        Enum.reduce(extractor.(nod), MapSet.new([id]), fn otherId, acc ->
            if otherId != :dnode_0 do
                MapSet.union(getAll(env, otherId, extractor), acc)
            else
                acc
            end
        end)
    end

    def getAllAncestor(env, id) do
        getAll(env, id, fn nod -> nod.parents end)
    end

    def getAllDescendant(env, id) do
        getAll(env, id, fn nod -> nod.children end)
    end

    def runWithAncestors(id) do 
        Agent.update(__MODULE__, fn env ->
            ancestors = MapSet.to_list(getAllAncestor(env, id))
            ancestors = ancestors -- (ancestors -- env.toRun)
            startAllMidNodes(env, ancestors -- env.children)
            startAllSources(env, env.children -- ( env.children -- ancestors ))
            %{ env | toRun: env.toRun -- ancestors }
        end)
    end

    def removeWithAncestors(id) do
        Agent.update(__MODULE__, fn env ->
            ancestors = MapSet.to_list(getAllAncestor(env, id))
            %{ env | 
                toRun: env.toRun -- ancestors, 
                children: env.children -- ancestors, 
                nodes: Map.drop(env.nodes, ancestors)
            }
        end)
    end

    def removeWithDescendants(ids) do
        Agent.update(__MODULE__, fn env ->
            IO.puts inspect ids
            Enum.reduce(ids, env, fn id, env ->
                if Map.has_key?(env.nodes, id) do
                    descendants = MapSet.to_list(getAllDescendant(env, id))
                    IO.puts "descendants #{inspect descendants}"
                    env = Enum.reduce(ids, env, fn id, env -> 
                        Enum.reduce(Map.get(env.nodes, id).parents, env, fn parentId, env ->
                            parentNode = Map.get(env.nodes, parentId)
                            %{ env | nodes: %{ env.nodes | parentId => %{ parentNode | children: parentNode.children -- [id] } } }
                        end)
                    end)
                    %{ env | 
                        children: env.children -- descendants, 
                        nodes: Map.drop(env.nodes, descendants)
                    }
                else
                    env
                end
            end)
        end)
    end

    def restartWithAncestors(id) do 
        Agent.get(__MODULE__, fn env ->
            ancestors = MapSet.to_list(getAllAncestor(env, id))
            Enum.map(ancestors, &Supervisor.terminate_child(DNode.Supervisor, &1))
            startAllMidNodes(env, ancestors -- env.children)
            startAllSources(env, env.children -- ( env.children -- ancestors ))
            :ok
        end)
    end

    def updateAllChildrenFromNode(nodes, obs) do
        Agent.get(__MODULE__, fn env -> 
            parents = Enum.reduce(nodes, MapSet.new(), &MapSet.union(&2, MapSet.new(Map.get(env.nodes, &1).parents)))
            Enum.map(parents, fn id -> 
                if id != obs.id do
                    DNode.updateChildren(id, Map.get(env.nodes, id).children)
                end
            end)
            %{ obs | children: Map.get(env.nodes, obs.id).children }
        end)
    end

    def createNode(parents, fct, initState \\ &nilState/1, reorderer \\ nil) do
        Agent.get_and_update(__MODULE__, &_addNode(&1, parents, fct, initState, false, reorderer))
    end

    def createSink(parents, fct) do
        Agent.get_and_update(__MODULE__, fn env -> _addNode(env, parents, fct,  &nilState/1, true) end)
        :ok
    end

    @errWrapper %{
        onNext: &__MODULE__._arity3ErrWrapper(&1), 
        onError: &__MODULE__._arity3ErrWrapper(&1), 
        onCompleted: &__MODULE__._arity2ErrWrapper(&1),
        onScheduled: &__MODULE__._arity2ErrWrapper(&1)
    }

    def nilState(_nod) do
        nil
    end

    def reorderComparator({t1, id1, eid1}, {t2, id2, eid2}) do
        case Timex.compare(t1, t2) do
            0 -> case RedBlackTree.compare_terms(id1, id2) do
                0 -> RedBlackTree.compare_terms(eid1, eid2)
                res -> res
            end
            res -> res
        end
    end

    def _reorderScheduled(nod, timeoutTime, timestamp) do
        case RedBlackTree.Utils.firstKV(nod.state.buff) do
            nil -> nod
            {{msgTime, sender, eid}, action} -> 
                if !Timex.before?(timeoutTime, msgTime) do # after or equal <=> not before
                    case action do
                        {:onNext, val} -> Drepel.onNextAs(nod, val, sender, msgTime)
                        {:onError, err} -> Drepel.onErrorAs(nod, err, sender, msgTime)
                        {:onCompleted} -> Drepel.onCompletedAs(nod, sender, msgTime)
                    end
                    _reorderScheduled(%{ nod | state: %{ nod.state | buff: RedBlackTree.delete(nod.state.buff, {msgTime, sender, eid}) } }, timeoutTime, timestamp)
                else
                    nod
                end
        end
    end

    def reorder(dnodes) do
        buffTime = 50
        Drepel.Env.createNode(dnodes, %{
            onNext: fn nod, sender, val, timestamp ->
                if timestamp==0 do
                    Drepel.onNextAs(nod, val, sender, timestamp)
                    nod
                else
                    Scheduler.schedule(nod, Timex.now, buffTime, timestamp, nil, nod.state.eid)
                    %{ nod | state: %{ nod.state | buff: RedBlackTree.insert(nod.state.buff, {timestamp, sender, nod.state.eid}, {:onNext, val}), eid: nod.state.eid+1 } }
                end
            end,
            onCompleted: fn nod, sender, timestamp ->
                if timestamp==0 do
                    Drepel.onCompletedAs(nod, sender, timestamp)
                    nod
                else
                    Scheduler.schedule(nod, Timex.now, buffTime, timestamp, nil, nod.state.eid)
                    %{ nod | state: %{ nod.state | buff: RedBlackTree.insert(nod.state.buff, {timestamp, sender, nod.state.eid}, {:onCompleted}), eid: nod.state.eid+1 } }
                end
            end,
            onError: fn nod, sender, err, timestamp ->
                if timestamp==0 do
                    Drepel.onErrorAs(nod, err, sender, timestamp)
                    nod
                else
                    Scheduler.schedule(nod, Timex.now, buffTime, timestamp, nil, nod.state.eid)
                    %{ nod | state: %{ nod.state | buff: RedBlackTree.insert(nod.state.buff, {timestamp, sender, nod.state.eid}, {:onError, err}), eid: nod.state.eid+1 } }
                end
            end, 
            onScheduled: &_reorderScheduled/3
        }, fn _nod -> %{ eid: 0, buff: RedBlackTree.new([], comparator: &reorderComparator/2)} end)
    end
    
    @doc """ 
        Mid node is can hook on "completed" or "error" event. If not the
        signal will be propagated to the children of this node.
        """
    def createMidNode(parents, fct, initState \\ &nilState/1, opts \\ []) do
        reorderer = if :proplists.get_value(:reorder, opts, false) and __MODULE__.get(:orderSensitive) do
            %MockDNode{id: id} = reorder(parents)
            id
        else
            nil
        end
        case fct do
            %{} -> createNode(parents, Enum.reduce(fct, %{}, fn {k, v}, acc -> Map.put(acc, k, Map.get(@errWrapper, k).(v)) end), initState, reorderer)
            _ -> createNode(parents, %{onNext: __MODULE__._arity3ErrWrapper(fct)}, initState, reorderer)
        end
    end

    def _arity2ErrWrapper(fct) do
        fct
        #fn obs, arg1 ->
        #    try do
        #        fct.(obs, arg1)
        #    catch
        #        err, errStr -> 
        #            Drepel.onError(obs, {err, errStr})
        #            obs
        #    end
        #end
    end

    def _arity3ErrWrapper(fct) do
        fct
        #fn obs, arg1, arg2 ->
        #    try do
        #        fct.(obs, arg1, arg2)
        #    catch
        #        err, errStr -> 
        #            Drepel.onError(obs, {err, errStr})
        #            obs
        #    end
        #end
    end
end