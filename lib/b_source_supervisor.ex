defmodule BSource.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%BSource{}=aSource) do
        Supervisor.start_child(__MODULE__, [aSource])
    end

    def restart(aSource, messages) do
        Supervisor.start_child(__MODULE__, [aSource, messages])
    end

    def init(:ok) do
        Supervisor.init([BSource], strategy: :simple_one_for_one)
    end

end