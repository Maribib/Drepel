defmodule TCPServer.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(args) do
        Supervisor.start_child(__MODULE__, [args])
    end

    def init(:ok) do
        Supervisor.init([TCPServer], strategy: :simple_one_for_one)
    end

end