defmodule Checkpoint do
    defstruct [lastCompleted: nil, clustNodes: [], buffs: %{}]

    use GenServer
    
    # Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodeName, sinks, clustNodes) do
    	GenServer.call({__MODULE__, nodeName}, {:reset, sinks, clustNodes})
    end

    def completed(nodeName, sink, chckptId) do
        GenServer.cast({__MODULE__, nodeName}, {:completed, sink, chckptId})
    end

    def setLastCompleted(nodeName, chckptId) do
        GenServer.call({__MODULE__, nodeName}, {:setLastCompleted, chckptId})
    end

    def lastCompleted do
        GenServer.call(__MODULE__, :lastCompleted)
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, sinks, clustNodes}, _from, state) do
    	{ :reply, :ok, 
            %{ state |
                buffs: Enum.reduce(sinks, %{}, &Map.put(&2, &1, 0)),
                clustNodes: clustNodes
            }
        }
    end

    def handle_call({:setLastCompleted, chckptId}, _from, state) do
        { :reply, :ok, %{ state | lastCompleted: chckptId } }
    end

    def handle_call(:lastCompleted, _from, state) do
        { :reply, state.lastCompleted, state }
    end

    def handle_cast({:completed, sink, chckptId}, state) do
        state = update_in(state.buffs[sink], &(&1 + 1))
        ready = Enum.reduce_while(state.buffs, true, fn {_, cnt}, acc ->
            ready = cnt>0
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            Enum.filter(state.clustNodes, &(&1!=node()))
            |> Enum.map(&Checkpoint.setLastCompleted(&1, chckptId))
            Enum.map(state.clustNodes, &Store.clean(&1, chckptId-1))
            {   
                :noreply,
                %{ state |
                    buffs: Enum.reduce(state.buffs, %{}, fn {id, cnt}, acc ->
                        Map.put(acc, id, cnt-1)
                    end)
                }
            }
        else
            {:noreply, %{state | lastCompleted: chckptId } }
        end
    end

end