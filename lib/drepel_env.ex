require DNode
require MockDNode

defmodule Drepel.Env do
    defstruct [ id: 1, children: [], nodes: %{}, workers: [], schedule: nil ]

    def new do
        Agent.start_link(fn -> %__MODULE__{} end, name: unquote(__MODULE__))
    end

    def get do
        Agent.get(__MODULE__, &(&1))
    end

    def get(key) do
        Agent.get(__MODULE__, &(Map.get(&1, key)))
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

    def _addNode(env, parents, runFct) do
        id = String.to_atom("dnode_#{env.id}")
        newDNode = %DNode{ id: id, parents: parents, runFct: runFct }
        env = Enum.reduce(parents, env, &_addChild/2 )
        { 
            %MockDNode{id: id}, 
            %{ env |  
                id: env.id+1, 
                nodes: Map.put(env.nodes, id, newDNode) 
            }
        }
    end

    def createNode(parents, fct) do
        Agent.get_and_update(__MODULE__, &_addNode(&1, parents, fct))
    end

    def createSink(parents, fct) do
        Agent.get_and_update(__MODULE__, &_addNode(&1, parents, fct))
        :ok
    end

    @errWrapper %{
        onNext: &__MODULE__._arity3ErrWrapper(&1), 
        onError: &__MODULE__._arity3ErrWrapper(&1), 
        onCompleted: &__MODULE__._arity2ErrWrapper(&1) 
    }
    
    @doc """ 
        Mid node is can hook on "completed" or "error" event. If not the
        signal will be propagated to the children of this node.
        """
    def createMidNode(parents, fct) do
        case fct do
            %{} -> createNode(parents, Enum.reduce(fct, %{}, fn {k, v}, acc -> Map.put(acc, k, Map.get(@errWrapper, k).(v)) end))
            _ -> createNode(parents, %{onNext: __MODULE__._arity3ErrWrapper(fct)})
        end
    end

    def _arity2ErrWrapper(fct) do
        fn obs, sender ->
            try do
                fct.(obs, sender)
            catch
                err, errStr -> Drepel.onError(obs, {err, errStr})
            end
        end
    end

    def _arity3ErrWrapper(fct) do
        fn obs, sender, val ->
            try do
                fct.(obs, sender, val)
            catch
                err, errStr -> Drepel.onError(obs, {err, errStr})
            end
        end
    end
end