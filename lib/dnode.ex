
defmodule DNode do
    @enforce_keys [:id]
    defstruct [ :id, parents: [], children: [], runFct: &Drepel.doNothing/1, endedParents: [] ]

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

    # Server API

    def init(%DNode{}=aDNode) do
        {:ok, aDNode}
    end

    def handle_cast({:onNext, sender, value}, aDNode) do
        case aDNode.runFct do
            %{onNextSink: nextFct} -> nextFct.(value)
            %{onNext: nextFct} -> nextFct.(aDNode, sender, value)
        end
        {:noreply, aDNode}
    end

    def stopIfNeeded(aDNode) do
        if length(aDNode.parents -- aDNode.endedParents)==0 do
            Manager.normalExit(self())
        end
    end

    def handle_cast({:onCompleted, sender}, aDNode) do
        # IO.puts "onCompleted"
        case aDNode.runFct do
            %{onCompletedSink: complFct} -> complFct.()
            %{onCompleted: complFct} -> complFct.(aDNode, sender)
            _ -> Enum.map(aDNode.children, &DNode.onCompleted(&1, aDNode.id)) # propagate
        end
        aDNode = %{ aDNode | endedParents: aDNode.endedParents ++ [sender] }
        stopIfNeeded(aDNode)
        {:noreply, aDNode}
    end

    def handle_cast({:onError, sender, err}, aDNode) do
        # IO.puts "onError"
        case aDNode.runFct do
            %{onErrorSink: errFct} -> errFct.(err)
            %{onError: errFct} -> errFct.(aDNode, sender, err)
            _ -> Enum.map(aDNode.children, &DNode.onError(&1, aDNode.id, err)) # propagate
        end
        aDNode = %{ aDNode | endedParents: aDNode.endedParents ++ [sender] }
        stopIfNeeded(aDNode)
        {:noreply, aDNode}
    end

end