defmodule Drepel.Stats do
    defstruct [ cnt: 0, sum: 0, max: 0 ]

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
    	{ :noreply, %{ stats | 
    		cnt: stats.cnt+1, 
    		sum: stats.sum+delta, 
    		max: stats.max>delta && stats.max || delta 
    	} }
    end

end