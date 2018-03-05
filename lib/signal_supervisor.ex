require Signal

defmodule Signal.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Signal{}=aSignal) do
        masterNode = node()
        case Map.get(aSignal.routing, aSignal.id) do
            ^masterNode -> Supervisor.start_child(__MODULE__, [aSignal])
            node -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Signal.Supervisor.start(aSignal)
                end)
                |> Task.await()
        end
    end

    def restart(node, id, chckptId, routing, repNodes, leader) do
        currNode = node()
        case node do
            ^currNode -> 
                aSignal = Store.get(id, chckptId)
                aSignal = %{ aSignal | 
                    repNodes: repNodes,
                    routing: routing, 
                    chckptId: chckptId+1,
                    leader: leader
                }
                Supervisor.start_child(__MODULE__, [aSignal])
            _ -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Signal.Supervisor.restart(node, id, chckptId, routing, repNodes, leader)
                end)
                |> Task.await()
        end
    end

    def init(:ok) do
        Supervisor.init([Signal], strategy: :simple_one_for_one)
    end

end