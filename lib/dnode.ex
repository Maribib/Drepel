
defmodule DNode do
    @enforce_keys [:id]
    defstruct [ :id, parents: [], children: [], runFct: &Drepel.doNothing/1, 
    initState: &Drepel.Env.nilState/0, state: nil, isSink: false ]

    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, aDNode) do
        GenServer.start_link(__MODULE__, aDNode, name: aDNode.id)
    end

    def onNext(id, sender, value) do
        GenServer.cast(id, {:onNext, sender, value})
    end

    def onCompleted(id, sender) do
        GenServer.cast(id, {:onCompleted, sender})
    end

    def onError(id, sender, err) do
        GenServer.cast(id, {:onError, sender, err})
    end

    def onScheduled(id, name) do
        GenServer.cast(id, {:onScheduled, name})
    end
    
    def runSource(id) do
        GenServer.cast(id, {:runSource})
    end

    def updateChildren(id, children) do
        GenServer.call(id, {:updateChildren, children})
    end

    # Server API

    def init(%DNode{}=aDNode) do
        { :ok, %{ aDNode | state: aDNode.initState.() } }
    end

    def handle_cast({:onNext, sender, value}, aDNode) do
        aDNode = case aDNode.runFct do
            %{onNextSink: nextFct} -> 
                nextFct.(value)
                aDNode
            %{onNext: nextFct} -> nextFct.(aDNode, sender, value)
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onCompleted, sender}, aDNode) do
        #IO.puts "onCompleted"
        aDNode = case aDNode.runFct do
            %{onCompletedSink: complFct} -> 
                complFct.()
                aDNode
            %{onCompleted: complFct} -> complFct.(aDNode, sender)
            _ -> 
                Drepel.onCompleted(aDNode) # propagate
                aDNode
        end
        if aDNode.isSink do
            Manager.normalExit(self())
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onError, sender, err}, aDNode) do
        #IO.puts "onError"
        aDNode = case aDNode.runFct do
            %{onErrorSink: errFct} -> 
                errFct.(err)
                aDNode
            %{onError: errFct} -> errFct.(aDNode, sender, err)
            _ -> 
                Drepel.onError(aDNode, err) # propagate
                aDNode
        end
        if aDNode.isSink do
            Manager.normalExit(self())
        end
        {:noreply, aDNode}
    end

    def handle_cast({:onScheduled, name}, aDNode) do
        aDNode = case aDNode.runFct do
            %{onScheduled: schedFct} -> schedFct.(aDNode, name)
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

    def handle_call({:updateChildren, children}, _from, aDNode) do
        {:reply, :ok, %{ aDNode | children: children } }
    end

end