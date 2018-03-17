require Logger

defmodule Checkpoint do
    defstruct [:timer, :sourcesRouting, :chckptInterval, :waiting,
    lastCompleted: -1, chckptId: 0, clustNodes: [], buffs: %{}]

    use GenServer

    def onComplete(state, chckptId) do
        if !is_nil(state.waiting) && state.waiting.id==chckptId do
            apply(&Drepel.Env.move/2, state.waiting.args)
            %{ state | waiting: nil }
        else
            state
        end
    end
    
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

    def setLastCompleted(nodeName, chckptId) do
        GenServer.call({__MODULE__, nodeName}, {:setLastCompleted, chckptId})
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
            timer: Process.send_after(self(), :injectChckpt, state.chckptInterval)
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
                waiting: nil
            }
        }
    end

    def handle_call({:setLastCompleted, chckptId}, _from, state) do
        { :reply, :ok, %{ state | lastCompleted: chckptId } }
    end

    def handle_call(:lastCompleted, _from, state) do
        { :reply, state.lastCompleted, state }
    end

    def handle_call({:injectAndWaitForCompletion, args}, _from, state) do
        Enum.map(state.sourcesRouting, &GenServer.cast(&1, {:checkpoint, state.chckptId}))
        { :reply, :ok, %{ state |
            chckptId: state.chckptId+1,
            waiting: %{ id: state.chckptId, args: args }
        } }
    end

    def handle_cast({:completed, sink, chckptId}, state) do
        state = update_in(state.buffs[sink], &(&1 + 1))
        ready = Enum.reduce_while(state.buffs, true, fn {_, cnt}, acc ->
            ready = cnt>0
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            Logger.info("checkpoint #{inspect chckptId} completed")
            # broadcast last completed checkpoint id
            Enum.filter(state.clustNodes, &(&1!=node()))
            |> Enum.map(&Checkpoint.setLastCompleted(&1, chckptId))
            # clean stores
            Enum.map(state.clustNodes, &Store.clean(&1, chckptId-1))
            # completion callback
            state = onComplete(state, chckptId)
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