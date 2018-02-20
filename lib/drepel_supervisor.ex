defmodule Drepel.Supervisor do
    use Supervisor

    def start_link(_opt \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
        children = [
            Supervisor.Spec.worker(Drepel.Env, [[name: Drepel.Env]]),
            Supervisor.Spec.worker(Drepel.Stats, [[name: Drepel.Stats]]),
            {Signal.Supervisor, name: Signal.Supervisor}, 
            {Source.Supervisor, name: Source.Supervisor},
            {EventSource.Supervisor, name: EventSource.Supervisor},
            {Node.Supervisor, name: Node.Supervisor},
            {Task.Supervisor, name: Task.Spawner},
            {Store, name: Store},
            {Checkpoint, name: Checkpoint},
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

end