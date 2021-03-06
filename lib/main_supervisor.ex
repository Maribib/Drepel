defmodule Main.Supervisor do
    use Supervisor

    def start_link(_opt \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
        children = [
            {Drepel.Env, name: Drepel.Env},
            #{Sampler, name: Sampler}, 
            {Signal.Supervisor, name: Signal.Supervisor}, 
            {Source.Supervisor, name: Source.Supervisor},
            {ClusterSupervisor, name: ClusterSupervisor},
            {Store, name: Store},
            {Checkpoint, name: Checkpoint},
            #{Balancer, name: Balancer},
            {TCPServer, name: TCPServer},
            {MyStuff, name: MyStuff}
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

end