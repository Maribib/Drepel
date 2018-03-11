defmodule Balancer do
    defstruct [:clustNodes, :timer]

    use GenServer

    @reportInterval 5000
    
    # Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(clustNodes) do
        GenServer.call(__MODULE__, {:reset, clustNodes})
    end

    def reset(node, clustNodes) do
        GenServer.call({__MODULE__, node}, {:reset, clustNodes})
    end

    def stop do
        GenServer.call(__MODULE__, :stop)
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, clustNodes}, _from, state) do
        { :reply, :ok, %{ state | 
            clustNodes: clustNodes,
            timer: Process.send_after(__MODULE__, :balance, @reportInterval)
        } }
    end

    def handle_call(:stop, _from, state) do
        Utils.cancelTimer(state.timer)
        { :reply, :ok, %{ state | timer: nil } }
    end

    def handle_info(:balance, state) do
        reports = Enum.reduce(state.clustNodes, %{}, &Map.put(&2, Drepel.Stats.getReport(&1)))

        { :noreply, %{ state |
            timer: Process.send_after(__MODULE__, :balance, @reportInterval)
        } }
    end
    
end