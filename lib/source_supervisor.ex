require Source

defmodule Source.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(fct) do
        Supervisor.start_child(__MODULE__, [fct])
    end

    def init(:ok) do
        Supervisor.init([Source], strategy: :simple_one_for_one)
    end
end