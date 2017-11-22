defmodule Drepel.Supervisor do
    use Supervisor

    def start_link(_opt \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
        children = [
            Supervisor.Spec.worker(Drepel.Env, [[name: Drepel.Env]]),
            {DNode.Supervisor, name: DNode.Supervisor}, 
            {Source.Supervisor, name: Source.Supervisor},
            {Task.Supervisor, name: Spawner.GenServer},

        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

end