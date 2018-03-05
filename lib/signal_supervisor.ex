require Signal

defmodule Signal.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Signal{}=aSignal, clustNodes, repFactor) do
        masterNode = node()
        case Map.get(aSignal.routing, aSignal.id) do
            ^masterNode -> Supervisor.start_child(__MODULE__, [aSignal, clustNodes, repFactor])
            node -> 
                t = Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Signal.Supervisor.start(aSignal, clustNodes, repFactor)
                end)
                Task.await(t)
        end
    end

    def restart(node, id, chckptId, routing, clustNodes, repFactor) do
        currNode = node()
        case node do
            ^currNode -> 
                aSignal = Store.get(id, chckptId)
                Supervisor.start_child(__MODULE__, [%{ aSignal | routing: routing, chckptId: chckptId+1}, clustNodes, repFactor])
            _ -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Signal.Supervisor.restart(node, id, chckptId, routing, clustNodes, repFactor)
                end)
                |> Task.await()
        end
    end

    def init(:ok) do
        Supervisor.init([Signal], strategy: :simple_one_for_one)
    end

end