defmodule Drepel.Stats do
    defstruct [ latency: %{cnt: 0, sum: 0, max: 0}, works: %{}, msgQ: %{}, sampling: false ]

    use GenServer
    
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

    # Server API

    def init(:ok) do
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
        Process.send_after(self(), :sample, 1000)
        { :reply, :ok, %{ stats | sampling: true } }
    end

    def handle_call(:stopSampling, _from, stats) do
        { :reply, :ok, %{ stats | sampling: false } }
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

    def handle_cast({:updateWork, {signal, _node}, delta}, stats) do
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
        if stats.sampling do
            Process.send_after(self(), :sample, 1000)
            { 
                :noreply, 
                Enum.reduce(Map.keys(stats.msgQ), stats, fn signal, stats ->
                    update_in(stats.msgQ[signal], fn samples ->
                        pid = Process.whereis(signal)
                        sample = Process.info(pid, :message_queue_len) |> elem(1)
                        samples ++ [sample]
                    end)
                end)
            }
        else
            { :noreply, stats }
        end
    end
end