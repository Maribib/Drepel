defmodule Source.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Source{}=aSource) do
        masterNode = node()
        case aSource.id do
            {_id, ^masterNode} -> Supervisor.start_child(__MODULE__, [aSource])
            {_id, node} -> 
                t = Task.Supervisor.async({Spawner.GenServer, node}, fn ->
                    if Process.whereis(Manager)==nil do
                        Task.Supervisor.start_child(Spawner.GenServer, fn -> Node.Supervisor.run(masterNode) end)
                    end
                    Source.Supervisor.start(aSource)
                end)
                Task.await(t)
        end
    end

    def init(:ok) do
        Supervisor.init([Source], strategy: :simple_one_for_one)
    end

end