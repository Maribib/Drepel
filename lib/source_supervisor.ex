defmodule Source.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Source{}=aSource) do
        currNode = node()
        case Map.get(aSource.routing, aSource.id) do
            ^currNode -> Supervisor.start_child(__MODULE__, [aSource])
            node -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Source.Supervisor.start(aSource)
                end)
                |> Task.await()
        end
    end

    def restart(aSource, node, id, chckptId) do
        currNode = node()
        case node do
            ^currNode -> 
                messages = Store.getMessages(id, chckptId)
                Supervisor.start_child(__MODULE__, [aSource, messages])
            _ -> 
                Task.Supervisor.async({Task.Spawner, node}, fn ->
                    Source.Supervisor.restart(aSource, node, id, chckptId)
                end)
                |> Task.await()
        end
    end

    def init(:ok) do
        Supervisor.init([Source], strategy: :simple_one_for_one)
    end

end