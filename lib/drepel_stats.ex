defmodule Drepel.Stats do
    defstruct [ :snapshot, :processes, :lastIn, :runningTime, msgQ: %{}, stopped: true ]

    use GenServer

    def sampleSchedulers do
        Enum.sort(:erlang.statistics(:scheduler_wall_time))
    end

    def computeUtilization(ts0, ts1) do
        Enum.map(
            Enum.zip(ts0, ts1), 
            fn {{i, a0, t0}, {i, a1, t1}} ->
                {i, (a1 - a0)/(t1 - t0)} 
            end)
      end
    
    # Client API

    def start_link(_opts) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(node, signals) do
    	GenServer.call({__MODULE__, node}, {:reset, signals})
    end

    def getReport(node) do
    	GenServer.call({__MODULE__, node}, :getReport)
    end

    def startSampling(node) do
        GenServer.call({__MODULE__, node}, :startSampling)
    end

    def stopSampling(node) do
        GenServer.call({__MODULE__, node}, :stopSampling)
    end

    def stopSampling do
        GenServer.call(__MODULE__, :stopSampling)
    end

    # Server API

    def init(:ok) do
        :erlang.system_flag(:scheduler_wall_time, true)
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, processes}, _from, _oldStats) do
    	{ 
            :reply, 
            :ok, 
            %__MODULE__{
                processes: processes,
                runningTime: Enum.reduce(processes, %{}, &Map.put(&2, &1, 0)),
                lastIn: %{},
                #msgQ: Enum.reduce(signals, %{}, &Map.put(&2, &1, []))
            } 
        }
    end

    def handle_call(:getReport, _from, state) do
        snapshot = sampleSchedulers()
        utilization = computeUtilization(state.snapshot, snapshot)
    	{ 
            :reply, 
            { utilization, state.runningTime }, 
            %{ state | 
                snapshot: snapshot,
                runningTime: Enum.reduce(state.processes, %{}, &Map.put(&2, &1, 0))
            } 
        }
    end

    def handle_call(:startSampling, _from, state) do
        Enum.map(state.processes, fn id ->
            Process.whereis(id)
            |> :erlang.trace(true, [:running, :timestamp]) 
        end)
        { :reply, :ok, %{ state | snapshot: sampleSchedulers(), stopped: false } }
    end

    def handle_call(:stopSampling, _from, state) do
        Enum.map(state.processes, fn id ->
            Process.whereis(id)
            |> :erlang.trace(false, [:running, :timestamp]) 
        end)
        { :reply, :ok, %{ state | stopped: true } }
    end

    def handle_info({:trace_ts, _, _, _, _}, %__MODULE__{stopped: true}=state) do
        { :noreply, state }
    end

    def handle_info({:trace_ts, pid, :in, _, timestamp}, %__MODULE__{stopped: false}=state) do
        { :noreply, put_in(state.lastIn[pid], timestamp) }
    end

    def handle_info({:trace_ts, pid, :out, _, {_, outSec, outUSec}}, %__MODULE__{stopped: false}=state) do
        {_, inSec, inUSec} = state.lastIn[pid]
        {
            :noreply, 
            update_in(state.runningTime[pid], fn cur -> 
                (is_nil(cur) && 0 || cur) + 1000000*(outSec - inSec) + (outUSec-inUSec)
            end) 
        }
    end
end