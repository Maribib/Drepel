require Signal

defmodule Signal.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Signal{}=aSignal) do
        masterNode = node()
        case aSignal.id do
            {_id, ^masterNode} -> Supervisor.start_child(__MODULE__, [aSignal])
            {_id, node} -> 
                t = Task.Supervisor.async({Spawner.GenServer, node}, fn ->
                    if Process.whereis(Manager)==nil do
                        Task.Supervisor.start_child(Spawner.GenServer, fn -> Node.Supervisor.run(masterNode) end)
                    end
                    Signal.Supervisor.start(aSignal)
                end)
                Task.await(t)
        end
    end

    def init(:ok) do
        Supervisor.init([Signal], strategy: :simple_one_for_one)
    end

end