defmodule Drepel.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
        children = [
            Supervisor.Spec.supervisor(DNode.Supervisor, [[name: DNode.Supervisor]]),
            Supervisor.Spec.supervisor(Source.Supervisor, [[name: Source.Supervisor]]),
            Supervisor.Spec.worker(Manager, [[], [name: Manager]]),
            Supervisor.Spec.worker(Scheduler, [[], [name: Scheduler]])
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end
end