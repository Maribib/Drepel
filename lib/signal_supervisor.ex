defmodule Signal.Supervisor do
    use DynamicSupervisor

    def start_link(_opts \\ nil) do
        DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start_child(aSignal) do
        DynamicSupervisor.start_child(__MODULE__, {Signal, aSignal})
    end

    def init(:ok) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

end