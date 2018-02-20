defmodule EventSource.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%EventSource{}=aSource, clusNodes, repFactor, chckptInterval) do
        currNode = node()
        case Map.get(aSource.routing, aSource.id) do
            ^currNode -> Supervisor.start_child(__MODULE__, [aSource, clusNodes, repFactor, chckptInterval])
            node -> 
                t = Task.Supervisor.async({Task.Spawner, node}, fn ->
                    EventSource.Supervisor.start(aSource, clusNodes, repFactor, chckptInterval)
                end)
                Task.await(t)
        end
    end

    def restart(aSource, node, id, chckptId, routing, clustNodes, repFactor, chckptInterval) do
        currNode = node()
        case node do
            ^currNode -> 
                messages = Store.getMessages(id, chckptId)
                Supervisor.start_child(__MODULE__, [%{ aSource | routing: routing }, clustNodes, repFactor, chckptInterval, messages])
            _ -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    EventSource.Supervisor.restart(aSource, node, id, chckptId, routing, clustNodes, repFactor, chckptInterval)
                end)
                |> Task.await()
        end
    end

    def init(:ok) do
        Supervisor.init([EventSource], strategy: :simple_one_for_one)
    end

end