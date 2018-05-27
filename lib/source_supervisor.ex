defmodule Source.Supervisor do
    use DynamicSupervisor

    def start_link(_opts \\ nil) do
        DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start_child(aSource) do
        module = aSource.__struct__
        DynamicSupervisor.start_child(__MODULE__, {module, aSource})
    end

    def start_child(aSource, messages) do
        module = aSource.__struct__
        DynamicSupervisor.start_child(__MODULE__, {module, {aSource, messages}})
    end

    def init(:ok) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

end