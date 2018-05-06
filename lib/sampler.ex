defmodule Sampler do
    defstruct [ :snapshot, :ids, :lastIn, :runningTime, :pidToId,
    :eventSources, :esPidToId,
    msgQ: %{}, stopped: true ]

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

    def reset(node, ids, eventSources) do
    	GenServer.call({__MODULE__, node}, {:reset, ids, eventSources})
    end

    def getReport(node) do
    	GenServer.call({__MODULE__, node}, :getReport)
    end

    def start(nodes) do
        Utils.multi_call(nodes, __MODULE__, :start)
    end

    def start do
        GenServer.call(__MODULE__, :start)
    end

    def stop(nodes) do
        Utils.multi_call(nodes, __MODULE__, :stop)
    end

    def stop do
        GenServer.call(__MODULE__, :stop)
    end

    # Server API

    def init(:ok) do
        :erlang.system_flag(:scheduler_wall_time, true)
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, ids, eventSources}, _from, _oldStats) do
    	{ 
            :reply, 
            :ok, 
            %__MODULE__{
                ids: ids,
                eventSources: eventSources,
                runningTime: %{},
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
            { 
                utilization, 
                Enum.reduce(state.runningTime, %{}, fn {pid, value}, acc -> 
                    key = if Map.has_key?(state.pidToId, pid) do
                        state.pidToId[pid]    
                    else
                        state.esPidToId[pid]
                    end
                    curr = Map.get(acc, key, 0)
                    Map.put(acc, key, curr+value)
                end)
            },
            %{ state | 
                snapshot: snapshot,
                runningTime: Enum.reduce(Map.keys(state.runningTime), %{}, &Map.put(&2, &1, 0))
            }
        }
    end

    def handle_call(:start, _from, state) do
        pidToId = Enum.reduce(state.ids, %{}, fn id, acc ->
            pid = Process.whereis(id)
            pid |> :erlang.trace(true, [:running, :timestamp]) 
            Map.put(acc, pid, id)
        end)
        esPidToId = Enum.reduce(state.eventSources, %{}, fn id, acc ->
            name = String.to_atom("tcp_#{Atom.to_string(id)}")
            pid = Process.whereis(name)
            pid |> :erlang.trace(true, [:running, :timestamp]) 
            Map.put(acc, pid, id)
        end)
        { :reply, :ok, %{ state |
            snapshot: sampleSchedulers(),
            stopped: false,
            pidToId: pidToId,
            esPidToId: esPidToId
        } }
    end

    def handle_call(:stop, _from, state) do
        Enum.map(state.ids, fn id ->
            Process.whereis(id)
            |> :erlang.trace(false, [:running, :timestamp]) 
        end)
        Enum.map(state.eventSources, fn id ->
            name = String.to_atom("tcp_#{Atom.to_string(id)}")
            Process.whereis(name)
            |> :erlang.trace(true, [:running, :timestamp]) 
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
        try do
            {_, inSec, inUSec} = state.lastIn[pid]
            {
                :noreply,
                update_in(state.runningTime[pid], fn cur -> 
                    (is_nil(cur) && 0 || cur) + 1000000*(outSec - inSec) + (outUSec-inUSec)
                end) 
            }
        rescue
            MatchError -> {:noreply, state}
        end
    end
end