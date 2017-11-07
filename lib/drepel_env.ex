require DNode
require MockDNode

defmodule Drepel.Env do
    defstruct [ id: 1, children: [], nodes: %{}, running: false, toRun: [] ]

    def new do
        Agent.start_link(fn -> %__MODULE__{} end, name: __MODULE__)
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

    def _addNode(env, parents, runFct, initState \\ &nilState/1, isSink \\ false) do
        id = String.to_atom("dnode_#{env.id}")
        newDNode = %DNode{ id: id, parents: parents, runFct: runFct, initState: initState, isSink: isSink }
        env = %{ env | toRun: env.toRun ++ [id] }
        env = Enum.reduce(parents, env, &_addChild/2 )
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

    def getAllAncestor(env, id) do
        node = Map.get(env.nodes, id)
        Enum.reduce(node.parents, MapSet.new([id]), fn parentId, acc ->
            if parentId != :dnode_0 do
                MapSet.union(getAllAncestor(env, parentId), acc)
            else
                acc
            end
        end)
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

    def createNode(parents, fct, initState \\ &nilState/1) do
        Agent.get_and_update(__MODULE__, &_addNode(&1, parents, fct, initState))
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
    
    @doc """ 
        Mid node is can hook on "completed" or "error" event. If not the
        signal will be propagated to the children of this node.
        """
    def createMidNode(parents, fct, initState \\ &nilState/1) do
        case fct do
            %{} -> createNode(parents, Enum.reduce(fct, %{}, fn {k, v}, acc -> Map.put(acc, k, Map.get(@errWrapper, k).(v)) end), initState)
            _ -> createNode(parents, %{onNext: __MODULE__._arity3ErrWrapper(fct)}, initState)
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