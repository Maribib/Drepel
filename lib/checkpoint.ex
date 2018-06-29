require Logger

defmodule Checkpoint do
    defstruct [:timer, :sourcesRouting, :chckptInterval,
    lastCompleted: -1, chckptId: 0, clustNodes: [], buffs: %{}]

    use GenServer
    
    # Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodeName, sinks, clustNodes, sourcesRouting, chckptInterval) do
    	GenServer.call({__MODULE__, nodeName}, {:reset, sinks, clustNodes, sourcesRouting, chckptInterval})
    end

    def completed(nodeName, sink, chckptId) do
        GenServer.cast({__MODULE__, nodeName}, {:completed, sink, chckptId})
    end

    def setLastCompleted(nodes, chckptId) do
        Utils.multi_call(nodes, __MODULE__, {:setLastCompleted, chckptId})
    end

    def lastCompleted do
        GenServer.call(__MODULE__, :lastCompleted)
    end

    def start do
        GenServer.call(__MODULE__, :start)
    end

    def start(node) do
        GenServer.call({__MODULE__, node}, :start)
    end

    def stop do
        GenServer.call(__MODULE__, :stop)
    end

    def stop(node) do
        GenServer.call({__MODULE__, node}, :stop)
    end

    def balance() do
        GenServer.cast(__MODULE__, :balance)
    end

    def injectAndWaitForCompletion(args) do
        GenServer.call(__MODULE__, {:injectAndWaitForCompletion, args})
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:start, _from, state) do
        { :reply, :ok, %{ state | 
            chckptId: state.lastCompleted==-1 && 0 || state.lastCompleted+1,
            timer: if state.chckptInterval>0 do
                Process.send_after(self(), :injectChckpt, state.chckptInterval)
            else
                nil
            end
        } }
    end

    def handle_call(:stop, _from, state) do
        Utils.cancelTimer(state.timer)
        { :reply, :ok, %{ state | timer: nil } }
    end

    def handle_call({:reset, sinks, clustNodes, sourcesRouting, chckptInterval}, _from, state) do
        { :reply, :ok, 
            %{ state |
                buffs: Enum.reduce(sinks, %{}, &Map.put(&2, &1, 0)),
                clustNodes: clustNodes,
                sourcesRouting: sourcesRouting,
                chckptInterval: chckptInterval,
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
            Logger.info("checkpoint #{inspect chckptId} completed")
            # multi_call last completed checkpoint id
            Enum.filter(state.clustNodes, &(&1!=node()))
            |> Checkpoint.setLastCompleted(chckptId)
            # clean stores
            Store.clean(state.clustNodes, chckptId-1)
            { :noreply, %{ state |
                buffs: Enum.reduce(state.buffs, %{}, fn {id, cnt}, acc ->
                    Map.put(acc, id, cnt-1)
                end),
                lastCompleted: chckptId
            } }
        else
            {:noreply, state }
        end
    end

    def handle_info(:injectChckpt, state) do
        Enum.map(state.sourcesRouting, &GenServer.cast(&1, {:checkpoint, state.chckptId}))
        { :noreply, %{ state | 
            chckptId: state.chckptId+1,
            timer: Process.send_after(self(), :injectChckpt, state.chckptInterval)
        } }
    end

end