defmodule Drepel.Stats do
    defstruct [ latency: %{cnt: 0, sum: 0, max: 0}, works: %{} ]

    use GenServer
    
    # Client API

    def start_link(_opts) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodeName) do
    	GenServer.call({ __MODULE__, nodeName }, :reset)
    end

    def get(nodeName) do
    	GenServer.call({__MODULE__, nodeName}, :get)
    end

    def updateLatency(delta) do
    	GenServer.cast(__MODULE__, {:updateLatency, delta})
    end

    def updateWork(from, delta) do
        GenServer.cast(__MODULE__, {:updateWork, from, delta})
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:reset, _from, _oldStats) do
    	{ :reply, :ok, %__MODULE__{} }
    end

    def handle_call(:get, _from, stats) do
    	{ :reply, stats, stats }
    end

    def handle_cast({:updateLatency, delta}, stats) do
    	{ 
            :noreply, 
            update_in(stats.latency, fn latency -> 
                %{ latency |
                    cnt: latency.cnt+1,
                    sum: latency.sum+delta, 
                    max: latency.max>delta && latency.max || delta 
                }
            end) 
        }
    end

    def handle_cast({:updateWork, from, delta}, stats) do
        { 
            :noreply, 
            update_in(stats.works, fn works -> 
                work = Map.get(works, from, %{cnt: 0, sum: 0})
                Map.put(works, from, %{ work |
                    cnt: work.cnt+1,
                    sum: work.sum+delta
                })
            end) 
        }
    end
end