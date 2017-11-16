defmodule EventCollector do
    use GenServer

    # Client API

    def start_link(_opts \\ nil) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def schedule(%MockDNode{id: id}, time, name \\ nil, onRun \\ nil, eid \\ nil) do
        %DNode{reorderer: reorderer} = Drepel.Env.getNode(id) 
        if is_nil(reorderer) do
            GenServer.cast(__MODULE__, {:schedule, %Event{id: id, time: time, name: name, onRun: onRun, eid: eid}})
        else 
            GenServer.cast(__MODULE__, {:schedule, %Event{id: reorderer, time: time, name: {:val, id, name}, onRun: onRun, eid: eid}})
        end
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