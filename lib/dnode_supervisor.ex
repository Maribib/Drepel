require DNode

defmodule DNode.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%DNode{}=aDNode) do
        Supervisor.start_child(__MODULE__, [aDNode])
    end

    def init(:ok) do
        Supervisor.init([DNode], strategy: :simple_one_for_one)
    end
end