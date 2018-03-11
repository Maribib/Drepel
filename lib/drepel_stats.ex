defmodule Drepel.Stats do
    defstruct [ :snapshot, :leader, :processes, :lastIn, :runningTime, msgQ: %{} ]

    use GenServer

    @samplingRate 1000

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

    def reset(node, signals, leader) do
    	GenServer.call({__MODULE__, node}, {:reset, signals, leader})
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

    def handle_call({:reset, processes, leader}, _from, _oldStats) do
    	{ 
            :reply, 
            :ok, 
            %__MODULE__{
                processes: processes,
                leader: leader,
                runningTime: Enum.reduce(processes, %{}, &Map.put(&2, &1, 0)),
                lastIn: Enum.reduce(processes, %{}, &Map.put(&2, &1, nil))
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
        { :reply, :ok, %{ state | snapshot: sampleSchedulers() } }
    end

    def handle_call(:stopSampling, _from, state) do
        Enum.map(state.processes, fn id ->
            Process.whereis(id)
            |> :erlang.trace(false, [:running, :timestamp]) 
        end)
        { :reply, :ok, state }
    end

    def handle_info({:trace_ts, pid, :in, _, timestamp}, state) do
        {:registered_name, name} = Process.info(pid, :registered_name)
        { :noreply, put_in(state.lastIn[name], timestamp) }
    end

    def handle_info({:trace_ts, pid, :out, _, {_, outSec, outUSec}}, state) do
        {:registered_name, name} = Process.info(pid, :registered_name)
        {_, inSec, inUSec} = state.lastIn[name]
        { 
            :noreply, 
            update_in(state.runningTime[name], fn cur -> 
                cur + 1000000*(outSec - inSec) + (outUSec-inUSec)
            end) 
        }
    end
end