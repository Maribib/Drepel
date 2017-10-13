defmodule EventCollector do
    use GenServer

    # Client API

    def start_link(_opts \\ nil) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def schedule(%MockDNode{id: id}, time, toRun, onRun \\ nil) do
        GenServer.cast(__MODULE__, {:schedule, %Event{id: id, time: time, toRun: toRun, onRun: onRun}})
    end

    def getEvents() do
        GenServer.call(__MODULE__, {:getEvents})
    end

    # Server API

    def init(:ok) do
        {:ok, []}
    end

    def handle_cast({:schedule, anEvent}, state) do
        { :noreply,  state ++ [ anEvent ] }
    end

    def handle_call({:getEvents}, from, state) do
        { :reply, state, state } 
    end
end