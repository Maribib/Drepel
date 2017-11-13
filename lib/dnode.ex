
defmodule DNode do
    @enforce_keys [:id]
    defstruct [ :id, parents: [], children: [], runFct: &Drepel.doNothing/1, 
    initState: &Drepel.Env.nilState/0, state: nil, isSink: false, timestamp: 0, 
    reorderer: nil, endedChildren: [] ]

    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, aDNode) do
        GenServer.start_link(__MODULE__, aDNode, name: aDNode.id)
    end

    def onNext(id, sender, value, timestamp) do
        GenServer.cast(id, {:onNext, sender, value, timestamp})
    end

    def onCompleted(id, sender, timestamp) do
        GenServer.cast(id, {:onCompleted, sender, timestamp})
    end

    def onError(id, sender, err, timestamp) do
        GenServer.cast(id, {:onError, sender, err, timestamp})
    end

    def onScheduled(id, name, timestamp) do
        GenServer.cast(id, {:onScheduled, name, timestamp})
    end
    
    def runSource(id) do
        GenServer.cast(id, {:runSource})
    end

    def updateChildren(id, children) do
        GenServer.call(id, {:updateChildren, children})
    end

    def childTerminate(id, childId) do
        GenServer.cast(id, {:childTerminate, childId})
    end

    # Server API

    def init(%DNode{}=aDNode) do
        Process.flag(:trap_exit, true)
        { :ok, %{ aDNode | state: aDNode.initState.(aDNode) } }
    end

    def terminate(reason, aDNode) do
        Enum.map(aDNode.parents, fn id -> childTerminate(id, aDNode.id) end)
        if !is_nil(aDNode.reorderer) do
            childTerminate(aDNode.reorderer, aDNode.id)
        end
        reason
    end

    def handle_cast({:onNext, sender, value, timestamp}, aDNode) do
        aDNode = %{ aDNode | timestamp: timestamp }
        aDNode = case aDNode.runFct do
            %{onNextSink: nextFct} -> 
                nextFct.(value)
                aDNode
            %{onNext: nextFct} -> nextFct.(aDNode, sender, value, timestamp)
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onCompleted, sender, timestamp}, aDNode) do
        #IO.puts "\n#{aDNode.id} onCompleted"
        aDNode = %{ aDNode | timestamp: timestamp }
        aDNode = case aDNode.runFct do
            %{onCompletedSink: complFct} -> 
                complFct.()
                aDNode
            %{onCompleted: complFct} -> complFct.(aDNode, sender, timestamp)
            _ -> 
                Drepel.onCompleted(aDNode) # propagate
                aDNode
        end
        if aDNode.isSink do
            Manager.normalExit(self())
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onError, sender, err, timestamp}, aDNode) do
        aDNode = %{ aDNode | timestamp: timestamp }
        #IO.puts "\nonError"
        aDNode = case aDNode.runFct do
            %{onErrorSink: errFct} -> 
                errFct.(err)
                aDNode
            %{onError: errFct} -> errFct.(aDNode, sender, err, timestamp)
            _ -> 
                Drepel.onError(aDNode, err) # propagate
                aDNode
        end
        if aDNode.isSink do
            Manager.normalExit(self())
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onScheduled, name, timestamp}, aDNode) do
        aDNode = %{ aDNode | timestamp: timestamp }
        aDNode = case aDNode.runFct do
            %{onScheduled: schedFct} -> schedFct.(aDNode, name, timestamp)
            _ -> aDNode
        end
        {:noreply, aDNode}
    end

    def handle_cast({:runSource}, aDNode) do
        aDNode = case aDNode.runFct do
            %{onNext: nextFct} -> nextFct.(aDNode)
            _ -> aDNode.runFct.(aDNode)
        end
        {:noreply, aDNode}
    end

    def handle_cast({:childTerminate, childId}, aDNode) do
        endedChildren = aDNode.endedChildren ++ [childId]
        if length(aDNode.children -- endedChildren)==0 do
            Manager.normalExit(self())
        end
        {:noreply, %{ aDNode | endedChildren: endedChildren } }
    end

    def handle_call({:updateChildren, children}, _from, aDNode) do
        {:reply, :ok, %{ aDNode | children: children } }
    end

end