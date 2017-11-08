defmodule Drepel.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
        children = [
            Supervisor.Spec.worker(Manager, [[], [name: Manager]]),
            Supervisor.Spec.worker(Schedule, [[], [name: Schedule]]),
            Supervisor.Spec.worker(Scheduler, [[], [name: Scheduler]]),
            Supervisor.Spec.supervisor(DNode.Supervisor, [[name: DNode.Supervisor]])
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

end