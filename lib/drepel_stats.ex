defmodule Drepel.Stats do
    defstruct [ :utilization, :timer, latency: %{cnt: 0, sum: 0, max: 0}, works: %{}, 
    msgQ: %{} ]

    use GenServer

    @samplingRate 1000

    def sampleSchedulers do
        Enum.sort(:erlang.statistics(:scheduler_wall_time))
    end

    def utilization(ts0, ts1) do
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

    def reset(nodeName, signals) do
    	GenServer.call({__MODULE__, nodeName}, {:reset, signals})
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

    def startSampling(nodeName) do
        GenServer.call({__MODULE__, nodeName}, :startSampling)
    end

    def stopSampling(nodeName) do
        GenServer.call({__MODULE__, nodeName}, :stopSampling)
    end

    def stopSampling do
        GenServer.call(__MODULE__, :stopSampling)
    end

    # Server API

    def init(:ok) do
        :erlang.system_flag(:scheduler_wall_time, true)
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, signals}, _from, _oldStats) do
    	{ 
            :reply, 
            :ok, 
            %__MODULE__{
                works: Enum.reduce(signals, %{}, &Map.put(&2, &1, %{cnt: 0, sum: 0})),
                msgQ: Enum.reduce(signals, %{}, &Map.put(&2, &1, []))
            } 
        }
    end

    def handle_call(:get, _from, stats) do
    	{ :reply, stats, stats }
    end

    def handle_call(:startSampling, _from, stats) do
        { 
            :reply, 
            :ok,
            %{ stats | 
                utilization: sampleSchedulers(),
                timer: Process.send_after(self(), :sample, @samplingRate)
            } 
        }
    end

    def handle_call(:stopSampling, _from, stats) do
        Utils.cancelTimer(stats.timer)
        { :reply, :ok, %{ stats | timer: nil } }
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

    def handle_cast({:updateWork, signal, delta}, stats) do
        { 
            :noreply, 
            update_in(stats.works[signal], fn work -> 
                %{ work |
                    cnt: work.cnt+1,
                    sum: work.sum+delta
                }
            end)
        }
    end

    def handle_info(:sample, stats) do    
        stats = Enum.reduce(Map.keys(stats.msgQ), stats, fn signal, stats ->
            update_in(stats.msgQ[signal], fn samples ->
                pid = Process.whereis(signal)
                sample = Process.info(pid, :message_queue_len) |> elem(1)
                samples ++ [sample]
            end)
        end)
        newUt = sampleSchedulers()
        IO.puts inspect utilization(stats.utilization, newUt)
        { 
            :noreply, 
            %{ stats | 
                utilization: newUt,
                timer: Process.send_after(self(), :sample, @samplingRate)
            }
        }
    end
end