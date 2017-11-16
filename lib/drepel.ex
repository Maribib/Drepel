require Drepel.Env, as: Env
require Drepel.Supervisor
require MockDNode
require DNode.Supervisor
require EventCollector

defmodule Sentinel do
    defstruct []
end

defmodule MockGroup do 
    @enforce_keys [:id]
    defstruct [:id]
end

defmodule MockWindow do
    @enforce_keys [:id, :start]
    defstruct [:id, :start]
end

defmodule Drepel do
    use Agent
    use Task

    defmacro __using__(args) do 
        quote do
            import unquote(__MODULE__)
            Env.new(unquote args)
            EventCollector.start_link()
        end
    end

    def doNothing(_ \\ nil) do
        nil
    end

    def run(duration \\ :inf) do
        {:ok, pid} = Drepel.Supervisor.start_link() 
        Drepel.Env.startAllNodes()
        Process.monitor(pid)
        case duration do
            :inf -> receive do
                _msg -> :done
            end
            _ -> receive do
                _msg -> :done
            after
                duration -> Supervisor.stop(Drepel.Supervisor)
            end
        end 
    end

    def onNext(%DNode{id: sender, children: children, timestamp: timestamp}, value) do 
        Enum.map(children, &DNode.onNext(&1, sender, value, timestamp))
    end

    def onNext(%DNode{id: sender, timestamp: timestamp}, children, value) do
        Enum.map(children, &DNode.onNext(&1, sender, value, timestamp))
    end

    def onCompleted(%DNode{id: sender, children: children, timestamp: timestamp}) do
        Enum.map(children, &DNode.onCompleted(&1, sender, timestamp))
        Manager.normalExit(self())
    end

    def onCompleted(%DNode{id: sender, timestamp: timestamp}, children) do
        Enum.map(children, &DNode.onCompleted(&1, sender, timestamp))
    end

    def onError(%DNode{id: sender, children: children, timestamp: timestamp}, err) do
        Enum.map(children, &DNode.onError(&1, sender, err, timestamp))
        Manager.normalExit(self())
    end

    def onNextAs(%DNode{children: children}, value, sender, timestamp) do
        Enum.map(children, &DNode.onNext(&1, sender, value, timestamp))
    end

    def onCompletedAs(%DNode{children: children}, sender, timestamp) do
        Enum.map(children, &DNode.onCompleted(&1, sender, timestamp))
    end

    def onErrorAs(%DNode{children: children}, err, sender, timestamp) do
        Enum.map(children, &DNode.onError(&1, sender, err, timestamp))
    end

    def ordered(%MockDNode{id: id}) do
        Drepel.Env.createNode([id], %{
            onNext: fn nod, _, _val, timestamp ->
                case nod.state do
                    %Sentinel{} -> %{ nod | state: timestamp }
                    _ -> 
                        isOrdered = Timex.diff(timestamp, nod.state)>=0
                        onNext(nod, isOrdered)
                        if isOrdered do
                            %{ nod | state: timestamp }
                        else
                            nod
                        end
                end
            end,
            onCompleted: fn nod, _, timestamp ->
                case nod.state do
                    %Sentinel{} -> %{ nod | state: timestamp }
                    _ -> 
                        isOrdered = Timex.diff(timestamp, nod.state)>=0
                        onNext(nod, isOrdered)
                        onCompleted(nod)
                        if isOrdered do
                            %{ nod | state: timestamp }
                        else
                            nod
                        end
                end
            end,
            onError: fn nod, _, err, timestamp ->
                case nod.state do
                    %Sentinel{} -> %{ nod | state: timestamp }
                    _ -> 
                        isOrdered = Timex.diff(timestamp, nod.state)>=0
                        onNext(nod, isOrdered)
                        onError(nod, err)
                        if isOrdered do
                            %{ nod | state: timestamp }
                        else
                            nod
                        end
                end
            end
        }, fn _ -> %Sentinel{} end)
    end

    def create(fct) do
        Drepel.Env.createNode([:dnode_0], fn nod ->
            fct.(nod)
            nod
        end)
    end

    def empty do
        create(fn nod -> 
            onCompleted(nod)
        end) 
    end

    def never do
        create(fn _ -> nil end)
    end

    def throw(err) do
        create(fn nod -> 
            onError(nod, err)
        end)
    end

    def just(value) do
        create(fn nod ->
            onNext(nod, value)
            onCompleted(nod)
        end)
    end

    def start(fct) do
        create(fn nod ->
            onNext(nod, fct.())
            onCompleted(nod)
        end)
    end

    def repeat(val, times) when is_integer(times) and times>=0 do
        create(fn nod ->
            case times do
               0 -> nil
               _ -> Enum.map(1..times, fn _ -> onNext(nod, val) end)
            end
            onCompleted(nod)
        end)
    end


    def _range(enum) do
        create(fn nod -> 
            Enum.map(enum, &onNext(nod, &1))
            onCompleted(nod)
        end)
    end
    def range(%Range{} = aRange), do: _range(aRange)
    def range(last) when is_integer(last), do: _range(0..last)
    def range(first, last) when is_integer(first) and is_integer(last), do: _range(first..last)

    def from(aList) when is_list(aList), do: _range(aList)
    def from(aString) when is_bitstring(aString), do: _range(String.graphemes(aString))
    def from(v1, v2 \\ nil, v3 \\ nil, v4 \\ nil, v5 \\ nil, v6 \\ nil, v7 \\ nil, v8 \\ nil, v9 \\ nil) do 
        _range(Enum.to_list(binding() |> Stream.filter(fn {_, b} -> !is_nil(b) end) |> Stream.map(fn {_, b} -> b end)))
    end

    def timer(offset, val \\ 0) do
        res = Drepel.Env.createNode([:dnode_0], %{ 
            onNext: fn nod -> nod end,
            onScheduled: fn nod, _, _ ->
                onNext(nod, val)
                onCompleted(nod)
                nod
            end
        })
        EventCollector.schedule(res, offset)
        res
    end

    def interval(duration) do
        res = Drepel.Env.createNode([:dnode_0], %{
            onNext: fn nod -> nod end,
            onScheduled: fn nod, _, _ ->
                onNext(nod, nod.state)
                %{ nod | state: nod.state+1 }
            end
        }, fn _ -> 0 end)
        EventCollector.schedule(res, duration, nil, fn event ->  
            %{ event | time: Timex.shift(event.time, microseconds: duration*1000) } 
        end)
        res
    end

    # MID NODES

    def map(%MockDNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            onNext(nod, fct.(val))
            nod
        end)
    end

    def _flatmap(nod, fct, val, id) do
        subObs = fct.(val)
        case subObs do
            %MockDNode{id: subId} ->
                Drepel.Env.addTmpChild(subId, id)
                Drepel.Env.runWithAncestors(subId)
                %{ nod | parents: nod.parents ++ [subId] }
            _ -> 
                onError(nod, "Flatmap argument must be a function that returns a node.")
                nod
        end 
    end

    def flatmap(%MockDNode{id: id}, fct) do    
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, sender, val, _ ->
                if sender==id do
                    nod = if length(nod.state.buff)==0 do
                        _flatmap(nod, fct, val, nod.id)
                    else
                        nod
                    end
                    %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val] } }
                else
                    onNext(nod, val)
                    nod
                end
            end,
            onCompleted: fn nod, sender, _ ->
                if sender==id do
                    case length(nod.state.buff) do
                        0 ->
                            onCompleted(nod)
                            nod
                        _ -> %{ nod | state: %{ nod.state | compl: true } }
                    end
                else
                    Drepel.Env.removeWithAncestors(sender)
                    [head | tail] = nod.state.buff
                    nod = %{ nod | state: %{ nod.state | buff: tail } }
                    if length(tail)>0 do
                        [head | _tail] = tail
                        _flatmap(nod, fct, head, nod.id)
                    else
                        if nod.state.compl do
                            onCompleted(nod)
                        end
                        nod
                    end
                end
            end
        }, fn _ -> %{ compl: false, buff: [] } end)
    end

    def scan(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            newState = fct.(val, nod.state)
            onNext(nod, newState)
            %{ nod | state: newState }
        end, fn _ -> init end)
    end

    def window(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        res = Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, _ ->
                case sender do
                    ^id1 -> 
                        onNext(nod, [{:val, val}])
                    ^id2 -> 
                        onNext(nod, [:close, :open])
                end
                nod
            end,
            onCompleted: fn nod, sender, _ ->
                case sender do
                    ^id1 -> onCompleted(nod)
                    ^id2 -> nil
                end
                nod
            end
        }, fn _ -> nil end, reorder: true)
        %MockWindow{id: res.id, start: :open}
    end

    def window(%MockDNode{}=nod, timespan) when is_integer(timespan) do
        window(nod, interval(timespan))
    end

    def windowWithCount(%MockDNode{id: id}, winSize) when is_integer(winSize) do
        res = Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            if nod.state>0 do
                onNext(nod, [{:val, val}])
                %{ nod | state: nod.state-1 }
            else
                onNext(nod, [:close, :open, {:val, val}])
                %{ nod | state: winSize-1 }
            end
        end, fn _ -> winSize end)
        %MockWindow{id: res.id, start: :open}
    end

    def slidingWindow(%MockDNode{id: id1}, %MockDNode{id: id2}, timespan) when is_integer(timespan) do
        res = Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, timestamp ->
                case sender do
                    ^id1 ->
                        onNext(nod, [{:val, val}])
                        nod
                    ^id2 -> 
                        Scheduler.schedule(nod, timestamp, timespan, nil, nil, nod.state)
                        onNext(nod, [:open])
                        %{ nod | state: nod.state+1 }
                end
            end,
            onScheduled: fn nod, _, _ ->
                onNext(nod, [:close])
                nod
            end
        }, fn _ -> 1 end, reorder: true)
        EventCollector.schedule(res, timespan, nil, nil, 0)
        %MockWindow{id: res.id, start: :open}
    end

    def slidingWindow(%MockDNode{}=nod, period, timespan) when is_integer(period) and is_integer(timespan) do
        slidingWindow(nod, interval(period), timespan)
    end

    def subscribe(%MockWindow{id: id, start: startState}, fct) do
        res = Drepel.Env.createMidNode([id], fn nod, _, actions, _  ->
            Enum.reduce(actions, nod, fn el, nod ->
                case el do
                    {:val, val} -> 
                        onNext(nod, val)
                        nod
                    :close -> 
                        [ head | tail ] = nod.state.buff
                        removedChildren = Enum.slice(nod.children, 0..head-1)
                        onCompleted(nod, removedChildren)
                        Drepel.Env.removeWithDescendants(removedChildren)
                        %{ nod | children: Enum.slice(nod.children, head..-1), state: %{ nod.state | buff: tail } }
                    :open -> 
                        oldChildren = Drepel.Env.getNode(nod.id).children
                        fct.(%MockDNode{id: nod.id}, nod.state.wid)
                        newChildren = Drepel.Env.getNode(nod.id).children -- oldChildren
                        nodes = Drepel.Env.startAllNodes()
                        nod = Drepel.Env.updateAllChildrenFromNode(nodes, nod)
                        %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [length(newChildren)], wid: nod.state.wid+1 } }
                end
            end)
        end, fn nod -> 
            len = length(nod.children)
            %{ buff: len==0 && [] || [len], wid: len==0 && 0 || 1 }
        end)
        if startState==:open do
            fct.(%MockDNode{id: res.id}, 0)
        end
        nil
    end

    def _buffer(id, boundId, init, %{onNextVal: nextValFct, onNextBound: nextBoundFct}) do
        Drepel.Env.createMidNode([id, boundId], fn nod, sender, val, _ ->
            case sender do
                ^id -> nextValFct.(nod, val)
                ^boundId -> nextBoundFct.(nod, val)
            end
        end, fn _ -> init end, reorder: true)
    end

    @doc """
    Implement bufferClosingSelector
    """
    def buffer(%MockDNode{id: id}, %MockDNode{id: boundId}) do
        _buffer(id, boundId, [], %{ 
            onNextVal: fn nod, val -> %{ nod | state: nod.state ++ [val] } end,
            onNextBound: fn nod, _ -> 
                onNext(nod, nod.state)
                %{ nod | state: [] }
            end
        })
    end

    def _ignoreWhenNil do 
        fn nod, val -> 
            case nod.state do
                nil -> nod
                _ -> %{ nod | state: nod.state ++ [val] }
            end
        end
    end

    def bufferBoundaries(%MockDNode{id: id}, %MockDNode{id: boundId}) do
        _buffer(id, boundId, nil, %{ 
            onNextVal: _ignoreWhenNil(),
            onNextBound: fn nod, _ -> 
                case nod.state do
                    nil -> nil
                    _ -> onNext(nod, nod.state)
                end
                %{ nod | state: [] }
            end
        })
    end

    def _updateBufferSwitch do
        fn buff -> 
            case buff do
                nil -> {nil, []}
                _ -> {buff, nil}
            end
        end
    end

    def bufferSwitch(%MockDNode{id: id}, %MockDNode{id: boundId}) do
        _buffer(id, boundId, nil, %{
            onNextVal: _ignoreWhenNil(),
            onNextBound: fn nod, _ ->
                case nod.state do
                    nil -> %{ nod | state: [] }
                    _ -> 
                        onNext(nod, nod.state)
                        %{ nod | state: nil }
                end
            end
        })
    end

    @initBuffWithCountState %{length: 0, buff: []}

    def bufferWithCount(%MockDNode{id: id}, count) do 
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            case nod.state.length+1 do
                ^count -> 
                    onNext(nod, nod.state.buff ++ [val])
                    %{ nod | state: @initBuffWithCountState }
                _ -> %{ nod | state: %{ nod.state | length: nod.state.length+1, buff: nod.state.buff ++ [val]} }
            end
        end, fn _ -> @initBuffWithCountState end)
    end

    def delay(%MockDNode{id: id}, timespan) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, timestamp ->
                Scheduler.schedule(nod, timestamp != 0 && timestamp || Timex.now, timespan, :onNext, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val], eid: nod.state.eid+1 } }
            end,
            onCompleted: fn nod, _, timestamp ->
                Scheduler.schedule(nod, timestamp != 0 && timestamp || Timex.now, timespan, :onCompleted, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | eid: nod.state.eid+1 } }
            end,
            onError: fn nod, _, err, timestamp ->
                Scheduler.schedule(nod, timestamp != 0 && timestamp || Timex.now, timespan, :onError, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [err], eid: nod.state.eid+1 } }
            end,
            onScheduled: fn nod, name, _ ->
                case name do
                    :onNext ->
                        [head | tail] = nod.state.buff
                        onNext(nod, head)
                        %{ nod | state: %{ nod.state | buff: tail } }
                    :onCompleted -> 
                        onCompleted(nod)
                        nod
                    :onError ->
                        [head | tail] = nod.state.buff
                        onError(nod, head)
                        %{ nod | state: %{ nod.state | buff: tail } }
                end
            end
        }, fn _ -> %{ buff: [], eid: 0 } end)
    end

    def tap(%MockDNode{id: id}, onNextFct, onErrorFct, onCompletedFct) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                onNextFct.(val)
                onNext(nod, val)
                nod
            end,
            onCompleted: fn nod, _, _ ->
                onCompletedFct.()
                onCompleted(nod)
                nod
            end,
            onError: fn nod, _, err, _ ->
                onErrorFct.(err)
                onError(nod, err)
                nod
            end
        })
    end

    def tapOnNext(%MockDNode{}=aDNode, onNextFct) do
        tap(aDNode, onNextFct, fn _err -> nil end, fn -> nil end)
    end
    def tapOnError(%MockDNode{}=aDNode, onErrorFct) do
        tap(aDNode, fn _val -> nil end, onErrorFct, fn -> nil end)
    end
    def tapOnCompleted(%MockDNode{}=aDNode, onCompletedFct) do
        tap(aDNode, fn _val -> nil end, fn _err -> nil end, onCompletedFct)
    end

    def materialize(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                onNext(nod, {&Drepel.onNext/2, val})
                nod
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, {&Drepel.onCompleted/1, nil})
                onCompleted(nod)
                nod
            end,
            onError: fn nod, _, err, _ ->
                onNext(nod, {&Drepel.onError/2, err})
                onCompleted(nod)
                nod
            end
        })
    end

    def dematerialize(%MockDNode{id: id}) do
        onNextFct = &Drepel.onNext/2
        onErrorFct = &Drepel.onError/2
        onCompletedFct = &Drepel.onCompleted/1
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                case val do
                    {^onNextFct, val} -> onNext(nod, val)
                    {^onErrorFct, err} -> onError(nod, err)
                    {^onCompletedFct, nil} -> onCompleted(nod)
                    _ -> onError(nod, "Elements must be be valid materialized value.")
                end
                nod
            end,
            onCompleted: fn nod, _, _ ->
                nod
            end,
            onError: fn nod, _, _err, _ ->
                nod
            end
        })
    end

    def timeInterval(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            now = Timex.now()
            onNext(nod, %{ interval: Timex.diff(now, nod.state)/1000, value: val})
            %{ nod | state: now }
        end, fn _ -> Timex.now end)
    end

    def timeout(%MockDNode{id: id}, timespan, errMsg \\ "Timeout.") when is_integer(timespan) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, timestamp ->
                if nod.state.done do
                    nod
                else
                    Scheduler.schedule(nod, timestamp, timespan, nod.state.eid, nil, nod.state)
                    onNext(nod, val)
                    %{ nod | state: %{ nod.state | eid: nod.state.eid+1 } }
                end
            end,
            onCompleted: fn nod, _, _ ->
                if nod.state.done do
                    nod
                else
                    onCompleted(nod)
                    %{ nod | state: %{ nod.state | done: true } }
                end
            end,
            onError: fn nod, _, err, _ ->
                if nod.state.done do
                    nod
                else
                    onError(nod, err)
                    %{ nod | state: %{ nod.state | done: true } }
                end
            end,
            onScheduled: fn nod, index, _ ->
                if !nod.state.done and index==nod.state.eid-1 do
                    onError(nod, errMsg)
                    %{ nod | state: %{ nod.state | done: true } }
                else
                    nod
                end
            end
        }, fn _ -> %{ eid: 0, done: false } end, reorder: true)
    end

    def reduce(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->  %{ nod | state: fct.(nod.state, val) } end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> init end)
    end

    def skip(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->  
            if (nod.state>0) do
                %{ nod | state: nod.state-1 }
            else
                onNext(nod, val)
                nod
            end
        end, fn _ -> nb end)
    end

    def skipLast(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if nod.state.size==nb do
                    [ head | tail ] = nod.state.buff
                    onNext(nod, head)
                    %{ nod | state: %{ nod.state | buff: tail ++ [val] } }
                else
                    %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val], size: nod.state.size+1 } }
                end
            end,
        }, fn _ -> %{ buff: [], size: 0 } end)
    end

    def elementAt(%MockDNode{id: id}, pos) when is_integer(pos) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->  
                if nod.state==0 do
                    onNext(nod, val)
                    onCompleted(nod)
                end
                %{ nod | state: nod.state-1 }
            end,
            onCompleted: fn nod, _, _ ->
                if nod.state>=0 do
                    onError(nod, "Argument out of range")
                end
                nod
            end
        }, fn _ -> pos end)
    end

    def _extractKey(v), do: v

    def distinct(%MockDNode{id: id}, extractKey \\ &_extractKey/1) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->  
            key = extractKey.(val)
            if (RedBlackTree.has_key?(nod.state, key)) do
                nod
            else
                onNext(nod, val)
                %{ nod | state: RedBlackTree.insert(nod.state, key) }
            
            end
        end, fn _ -> RedBlackTree.new() end)
    end

    def ignoreElements(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], fn nod, _, _val, _ ->  
            nod
        end)
    end

    def sample(%MockDNode{id: id}, timespan) when is_integer(timespan) do
        %MockDNode{id: samplerId} = interval(timespan)
        Drepel.Env.createMidNode([id, samplerId], %{
            onNext: fn nod, sender, val, _ ->
                case sender do
                    ^id -> %{ nod | state: %{ nod.state | val: val } }
                    ^samplerId -> 
                        if !nod.state.compl do
                            case nod.state.val do
                                %Sentinel{} -> nod
                                _ ->
                                    onNext(nod, nod.state.val)
                                    %{ nod | state: %{ nod.state | val: %Sentinel{} } }
                            end
                        else
                            nod
                        end
                end
            end,
            onCompleted: fn nod, _, _ ->
                onCompleted(nod)
                %{ nod | state: %{ nod.state | compl: true } }
            end,
            onError: fn nod, _, err, _ ->
                onError(nod, err)
                %{ nod | state: %{ nod.state | compl: true } }
            end
        }, fn _ -> %{ val: %Sentinel{}, compl: false } end, reorder: true)
    end

    def filter(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->  
            if condition.(val) do
                onNext(nod, val)
            end
            nod
        end)
    end

    def _trueCond(_val) do
        true
    end

    def first(%MockDNode{id: id}, condition \\ &_trueCond/1) do
        Drepel.Env.createMidNode([id], %{ 
            onNext: fn nod, _, val, _ ->  
                if nod.state and condition.(val) do
                    onNext(nod, val)
                    onCompleted(nod)
                    %{ nod | state: false}
                else
                    nod
                end
            end,
            onCompleted: fn nod, _, _ ->
                if nod.state do
                    onError(nod, "Any element match condition.")
                end
                nod
            end
        }, fn _ -> true end)
    end

    def last(%MockDNode{id: id}, condition \\ &_trueCond/1) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if condition.(val) do
                    %{ nod | state: val }
                else
                    nod
                end
            end,
            onCompleted: fn nod, _, _ ->
                case nod.state do
                    %Sentinel{} -> onError(nod, "Any element match condition.")
                    _ -> 
                        onNext(nod, nod.state)
                        onCompleted(nod)
                end
                nod
            end
        }, fn _ -> %Sentinel{} end)
    end

    def debounce(%MockDNode{id: id}, timespan) do
        Drepel.Env.createMidNode([id], %{ 
            onNext: fn nod, _, val, timestamp -> 
                Scheduler.schedule(nod, timestamp, timespan, :onNext, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | count: nod.state.count+1, val: val, eid: nod.state.eid+1 } }
            end,
            onCompleted: fn nod, _, timestamp ->
                Scheduler.schedule(nod, timestamp, timespan, :onCompleted, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | eid: nod.state.eid+1 } }
            end,
            onScheduled: fn nod, name, _ ->
                case name do
                    :onNext ->
                        if nod.state.count==1 do
                            onNext(nod, nod.state.val)
                        end
                        %{ nod | state: %{ nod.state | count: nod.state.count-1 } }
                    :onCompleted ->
                        onCompleted(nod)
                        nod
                end
            end
        }, fn _ -> %{ val: %Sentinel{}, count: 0, eid: 0 } end, reorder: true)
    end

    def take(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if nod.state>0 do
                    onNext(nod, val)
                end
                if nod.state==0 do
                    onCompleted(nod)
                end
                %{ nod | state: nod.state-1 }
            end,
            onCompleted: fn nod, _, _ ->
                nod
            end
        }, fn _ -> nb end)  
    end

    def takeLast(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if nod.state.size==nb do
                    [_ | tail] = nod.state.buff
                    %{ nod | state: %{ nod.state | buff: tail ++ [val] } }
                else
                    %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val], size: nod.state.size+1 } }
                end
            end,
            onCompleted: fn nod, _, _ ->
                Enum.map(nod.state.buff, &onNext(nod, &1))
                onCompleted(nod)
                nod
            end
        }, fn _ -> %{ buff: [], size: 0 } end)
    end

    def merge(dnodes) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, _, val, _ ->
                onNext(nod, val)
                nod
            end,
        onCompleted: fn nod, sender, _ ->
                compl = nod.state.compl ++ [sender]
                if length(nod.parents -- compl)==0 do
                    if nod.state.errBuff == [] do
                        onCompleted(nod)
                    else
                        onError(nod, nod.state.errBuff)
                    end
                end
                %{ nod | state: %{ nod.state | compl: compl } }
            end,
            onError: fn nod, sender, err, _ ->
                compl = nod.state.compl ++ [sender]
                if length(nod.parents -- compl)==0 do
                    onError(nod, nod.state.errBuff ++ [err])
                end
                %{ nod | state: %{ nod.state | 
                    errBuff: nod.state.errBuff ++ [err], 
                    compl: compl} 
                }
            end
        }, fn _ -> %{ errBuff: [], compl: [] } end, reorder: true)
    end

    def merge(%MockDNode{}=dnode1, %MockDNode{}=dnode2) do
        merge([dnode1, dnode2])
    end

    def zip(dnodes, zipFct) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val, _ ->
                aQueue = Map.get(nod.state.buffs, sender)
                isReady = :queue.len(aQueue)==0
                nod = %{ nod | state: %{ nod.state | 
                    buffs: %{ nod.state.buffs | sender => :queue.in(val, aQueue) }, 
                    ready: nod.state.ready+(isReady && 1 || 0) 
                } }
                if nod.state.ready==nod.state.nbParents do
                    {ready, args, buffs, compl} = Enum.reduce(nod.state.buffs, {0, [], %{}, false}, fn {nodeId, aQueue}, {ready, args, buffs, compl} ->
                        {{:value, val}, aQueue} = :queue.out(aQueue)
                        isEmpty = :queue.is_empty(aQueue)
                        { 
                            ready+(isEmpty && 0 || 1), args ++ [val], 
                            Map.put(buffs, nodeId, aQueue), 
                            compl or (isEmpty and Map.get(nod.state.compls, nodeId)) 
                        }
                    end)
                    onNext(nod, apply(zipFct, args))
                    if compl do
                        if length(nod.state.errBuff)>0 do
                            onError(nod, nod.state.errBuff)
                        else
                            onCompleted(nod)
                        end
                    end 
                    %{ nod | state: %{ nod.state | buffs: buffs, ready: ready, compl: compl } }
                else
                    nod
                end
            end,
            onCompleted: fn nod, sender, _ ->
                if !nod.state.compl do
                    if :queue.is_empty(Map.get(nod.state.buffs, sender)) do
                        onCompleted(nod)
                        %{ nod | state: %{ nod.state | compl: true } }
                    else
                        %{ nod | state: %{ nod.state | compls: %{ nod.state.compls | sender => true } } }
                    end
                else
                    nod                   
                end
            end,
            onError: fn nod, sender, err, _ ->
                if !nod.state.compl do
                    if :queue.is_empty(Map.get(nod.state.buffs, sender)) do
                        onError(nod, nod.state.errBuff ++ [err])
                        %{ nod | state: %{ nod.state | compl: true } }
                    else
                        %{ nod | state: %{ nod.state | 
                            compls: %{ nod.state.compls | sender => true }, 
                            errBuff: nod.state.errBuff ++ [err] 
                        } }
                    end
                else
                    nod                   
                end
            end
        }, fn nod -> %{ 
            errBuff: [],
            compl: false, 
            ready: 0, 
            nbParents: length(nod.parents), 
            buffs: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, :queue.new()) end),
            compls: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, false) end) 
        } end)
    end

    def zip(%MockDNode{}=dnode1, %MockDNode{}=dnode2, zipFct) do
        zip([dnode1, dnode2], zipFct)
    end

    def dcatch(%MockDNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, sender, val, _ ->
                if (nod.state and sender==id) or (!nod.state and sender != id) do
                    onNext(nod, val)
                end
                nod
            end,
            onError: fn nod, sender, err, _ ->
                if sender==id do
                    subObs = fct.()
                    case subObs do
                        %MockDNode{id: subId} ->
                            Drepel.Env.addTmpChild(subId, nod.id)
                            Drepel.Env.runWithAncestors(subId)
                            %{ nod | parents: nod.parents ++ [subId], state: false }
                        _ -> 
                            onError(nod, "Catch argument must be a function that returns a node.")
                            nod
                    end 
                else
                    onError(nod, err)
                    nod
                end
            end
        }, fn _ -> true end)
    end

    def retry(%MockDNode{id: id}, number \\ :inf) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                onNext(nod, val)
                nod
            end,
            onError: fn nod, sender, err, _ ->
                if nod.state==:inf or nod.state>0 do
                    Drepel.Env.restartWithAncestors(sender)
                    if nod.state != :inf do
                        %{ nod | state: nod.state-1 }
                    else
                        nod
                    end
                else
                    if nod.state != :inf do
                        onError(nod, err)
                    end
                    nod
                end
            end
        }, fn _ -> number end)
    end

    def combineLatest(dnodes, combineFct) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val, _ ->
                state = %{ nod.state | 
                    vals: %{ nod.state.vals | sender => val }, 
                    ready: nod.state.ready-(Map.get(nod.state.vals, sender)==%Sentinel{} && 1 || 0) 
                }
                if (state.ready==0) do
                    onNext(nod, apply(combineFct, Enum.map(nod.parents, fn id -> Map.get(state.vals, id) end)))
                end
                %{ nod | state: state }
            end,
            onCompleted: fn nod, sender, _ ->
                if Map.get(nod.state.compls, sender) do
                    nod
                else
                    if (nod.state.compl==1) do
                        if length(nod.state.errBuff)>0 do
                            onError(nod, nod.state.errBuff)
                        else
                            onCompleted(nod)
                        end
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true },
                        compl: nod.state.compl+1 
                    } }
                end
            end,
            onError: fn nod, sender, err, _ ->
                if Map.get(nod.state.compls, sender) do
                    nod
                else
                    if (nod.state.compl==1) do
                        onError(nod, nod.state.errBuff ++ [err])
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true }, 
                        compl: nod.state.compl+1, 
                        errBuff: nod.state.errBuff ++ [err] 
                    } }
                end
            end
        }, fn nod -> %{
            errBuff: [],
            ready: length(nod.parents), 
            compl: length(nod.parents), 
            vals: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, %Sentinel{}) end),
            compls: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, false) end)
        } end, reorder: true)
    end

    def combineLatest(%MockDNode{}=dnode1, %MockDNode{}=dnode2, combineFct) do
        combineLatest([dnode1, dnode2], combineFct)
    end

    def join(%MockDNode{id: id1}, %MockDNode{id: id2}, timespan1, timespan2, joinFct) when is_integer(timespan1) and is_integer(timespan2) and is_function(joinFct) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, timestamp ->
                buff = case sender do
                    ^id1 -> 
                        Scheduler.schedule(nod, timestamp, timespan1, id1, nil, nod.state.eid)
                        Enum.map(Map.get(nod.state.buff, id2), &onNext(nod, joinFct.(val, &1)))
                        %{ nod.state.buff | id1 => Map.get(nod.state.buff, id1) ++ [val] }
                    ^id2 -> 
                        Scheduler.schedule(nod, timestamp, timespan2, id2, nil, nod.state.eid)
                        Enum.map(Map.get(nod.state.buff, id1), &onNext(nod, joinFct.(&1, val)))
                        %{ nod.state.buff | id2 => Map.get(nod.state.buff, id2) ++ [val] }
                end
                %{ nod | state: %{ nod.state | buff: buff, eid: nod.state.eid+1 } }
            end,
            onCompleted: fn nod, sender, _ ->
                if !Map.get(nod.state.compls, sender) do
                    if nod.state.compl==1 do
                        if length(nod.state.errBuff)>0 do
                            onError(nod, nod.state.errBuff)
                        else
                            onCompleted(nod)
                        end
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true }, 
                        compl: nod.state.compl+1, 
                    } }
                else
                    nod
                end
            end,
            onError: fn nod, sender, err, _ ->
                if !Map.get(nod.state.compls, sender) do
                    if nod.state.compl==1 do
                        onError(nod, nod.state.errBuff ++ err)
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true }, 
                        compl: nod.state.compl+1, 
                        errBuff: nod.state.errBuff ++ [err]
                    } }
                else
                    nod
                end
            end,
            onScheduled: fn nod, id, _ ->
                update_in(nod.state.buff[id], &tl(&1))
            end
        }, fn _ -> %{ 
            compl: 0,
            compls: %{ id1 => false, id2 => false }, 
            errBuff: [], 
            buff: %{ id1 => [], id2 => [] }, 
            eid: 0 
        } end, reorder: true)
    end

    def startWith(%MockDNode{id: id}, v1, v2 \\ nil, v3 \\ nil, v4 \\ nil, v5 \\ nil, v6 \\ nil, v7 \\ nil, v8 \\ nil, v9 \\ nil) do
        values = Enum.to_list(binding() |> tl() |> Stream.filter(fn {_, b} -> !is_nil(b) end) |> Stream.map(fn {_, b} -> b end))
        res = Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ -> 
                Enum.map(nod.state, &onNext(nod, &1))
                onNext(nod, val)
                %{ nod | state: [] }
            end,
            onError: fn nod, _, err, _ ->
                Enum.map(nod.state, &onNext(nod, &1))
                onError(nod, err)
                %{ nod | state: [] }
            end,
            onCompleted: fn nod, _, _ ->
                Enum.map(nod.state, &onNext(nod, &1))
                onCompleted(nod)
                %{ nod | state: [] }
            end,
            onScheduled: fn nod, _, _ -> 
                Enum.map(nod.state, &onNext(nod, &1))
                %{ nod | state: [] }
            end
        }, fn _ -> 
            IO.puts inspect values
            values 
        end)
        EventCollector.schedule(res, 0)
        res
    end

    def every(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ -> 
                %{ nod | state: nod.state && condition.(val) }
            end,
            onError: fn nod, _, err, _ ->
               onNext(nod, nod.state)
               onError(nod, err)
               nod
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> true end)
    end

    def amb(dnodes) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val, _ -> 
                case nod.state do
                    %Sentinel{} -> 
                        onNext(nod, val)
                        %{ nod | state: sender }
                    _ -> 
                        if nod.state==sender do
                            onNext(nod, val)
                        end
                        nod
                end
            end,
            onError: fn nod, sender, err, _ ->
                case nod.state do
                    %Sentinel{} -> 
                        onError(nod, err)
                        %{ nod | state: sender }
                    _ -> 
                        if (nod.state==sender) do
                            onError(nod, err)
                        end
                        nod
                end
            end,
            onCompleted: fn nod, sender, _ ->
                case nod.state do
                    %Sentinel{} -> 
                        onCompleted(nod)
                        %{ nod | state: sender }
                    _ -> 
                        if nod.state==sender do
                            onCompleted(nod)
                        end
                        nod
                end
            end
        }, fn _ -> %Sentinel{} end, reorder: true)
    end

    def amb(%MockDNode{}=dnode1, %MockDNode{}=dnode2) do
        amb([dnode1, dnode2])
    end

    def contains(%MockDNode{id: id}, anElement) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if !nod.state and val==anElement do
                    onNext(nod, true)
                    onCompleted(nod)
                    %{ nod | state: true}
                else
                    nod
                end
            end,
            onError: fn nod, _, err, _ ->
                if !nod.state do
                    onNext(nod, false)
                    onError(nod, err)
                    %{ nod | state: true}
                else
                    nod
                end
            end,
            onCompleted: fn nod, _, _ ->
                if !nod.state do
                    onNext(nod, false)
                    onCompleted(nod)
                    %{ nod | state: true}
                else
                    nod
                end
            end
        }, fn _ -> false end)
    end

    def defaultIfEmpty(%MockDNode{id: id}, defaultEl) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                onNext(nod, val)
                %{ nod | state: true } 
            end,
            onError: fn nod, _, err, _ ->
                if !nod.state do
                    onNext(nod, defaultEl)
                end
                onError(nod, err)
                nod
            end,
            onCompleted: fn nod, _, _ ->
                if !nod.state do
                    onNext(nod, defaultEl)
                end
                onCompleted(nod)
                nod
            end
        }, fn _ -> false end)
    end

    def sequenceEqual(dnodes) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val, _ ->
                if nod.state.isEqual do
                    index = Map.get(nod.state.indexes, sender)
                    if index<nod.state.length do
                        if Enum.at(nod.state.buff, index)!=val do
                            update_in(nod.state.isEqual, fn _ -> false end)
                        else
                            compared = Enum.at(nod.state.compared, index)
                            nod = %{ nod | state: %{ nod.state | 
                                indexes: %{ nod.state.indexes | sender => index+1 }, 
                                compared: List.insert_at(nod.state.compared, index, compared+1) 
                            } }
                            if index==0 and compared+1==nod.state.nbParents do
                                %{ nod | state: 
                                    %{ nod.state | 
                                        compared: tl(nod.state.compared), 
                                        indexes: Enum.reduce(nod.state.indexes, %{}, fn {id, index}, acc -> Map.put(acc, id, index-1) end),
                                        length: nod.state.length-1,
                                        buff: tl(nod.state.buff)
                                    } 
                                }
                            else
                                nod
                            end
                        end
                    else
                        %{ nod | state: %{ nod.state | 
                            buff: nod.state.buff ++ [val], 
                            length: nod.state.length+1, 
                            compared: nod.state.compared ++ [1], 
                            indexes: %{ nod.state.indexes | sender => index+1 } 
                        } }
                    end
                else
                    nod
                end
            end,
            onError: fn nod, sender, err, _ ->
                if !Map.get(nod.state.compls, sender) do
                    if nod.state.compl+1==nod.state.nbParents do
                        onNext(nod, nod.state.isEqual and nod.state.length==0)
                        onError(nod, nod.state.errBuff ++ [err])
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true }, 
                        compl: nod.state.compl+1, 
                        errBuff: nod.state.errBuff ++ [err] 
                    } }
                else
                    nod
                end
            end,
            onCompleted: fn nod, sender, _ ->
                 if !Map.get(nod.state.compls, sender) do
                    if nod.state.compl+1==nod.state.nbParents do
                        onNext(nod, nod.state.isEqual and nod.state.length==0)
                        if length(nod.state.errBuff)>0 do
                            onError(nod, nod.state.errBuff)
                        else
                            onCompleted(nod)
                        end
                    end
                    %{ nod | state: %{ nod.state | 
                        compls: %{ nod.state.compls | sender => true }, 
                        compl: nod.state.compl+1 
                    } }
                else
                    nod
                end
            end
        }, fn nod -> %{
            errBuff: [],
            compl: 0,
            compls: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, false) end),
            nbParents: length(nod.parents),
            isEqual: true,
            buff: [],
            length: 0,
            compared: [],
            indexes: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, 0) end)
        } end) 
    end

    def sequenceEqual(%MockDNode{}=dnode1, %MockDNode{}=dnode2) do
        sequenceEqual([dnode1, dnode2])
    end

    def skipUntil(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, _ ->
                case sender do
                    ^id1 ->
                        if nod.state do
                            onNext(nod, val)
                        end
                        nod
                    ^id2 -> %{ nod | state: true }
                end
            end,
            onCompleted: fn nod, sender, _ ->
                if sender==id1 do
                    onCompleted(nod)
                end
                nod
            end,
            onError: fn nod, sender, err, _ ->
                if sender==id1 do
                    onError(nod, err)
                end
                nod
            end
        }, fn _ -> false end, reorder: true)  
    end

    def skipWhile(%MockDNode{id: id}, skipFct) do
        Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            if nod.state do
                onNext(nod, val)
                nod
            else
                if !skipFct.(val) do
                    onNext(nod, val)
                    %{ nod | state: true }
                else 
                    nod
                end
            end
        end, fn _ -> false end)  
    end

    def takeUntil(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, _ ->
                case sender do
                    ^id1 ->
                        if nod.state do
                            onNext(nod, val)
                        end
                        nod
                    ^id2 -> 
                        onCompleted(nod)
                        %{ nod | state: false }
                end
            end,
            onCompleted: fn nod, sender, _ ->
                if nod.state and sender==id1 do
                    onCompleted(nod)
                end
                nod
            end,
            onError: fn nod, sender, err, _ ->
                if nod.state and sender==id1 do
                    onError(nod, err)
                end
                nod
            end
        }, fn _ -> true end, reorder: true)
    end

    def takeWhile(%MockDNode{id: id}, takeFct) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if nod.state do
                    if !takeFct.(val) do
                        onCompleted(nod)
                        %{ nod | state: false }
                    else
                        onNext(nod, val)
                        nod
                    end
                else
                    nod
                end
            end, 
            onCompleted: fn nod, _, _ ->
                if nod.state do
                    onCompleted(nod)
                    %{ nod | state: false }
                else 
                    nod
                end
            end, onError: fn nod, _, err, _ ->
                if nod.state do
                    onError(nod, err)
                    %{ nod | state: false }
                else 
                    nod
                end
            end
        }, fn _ -> true end)  
    end

    def _extremum(id, comparator) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                case nod.state do
                    %Sentinel{} -> %{ nod | state: val }
                    _ -> 
                        if comparator.(val, nod.state) do
                            %{ nod | state: val }
                        else
                            nod
                        end
                end
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> %Sentinel{} end)  
    end

    def max(%MockDNode{id: id}) do
        _extremum(id, fn v1, v2 -> v1>v2 end)
    end

    def min(%MockDNode{id: id}) do
        _extremum(id, fn v1, v2 -> v1<v2 end)
    end

    def _extremumBy(id, extractor, comparator) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                key = extractor.(val)
                case nod.state do
                    %Sentinel{} -> %{ nod | state: %{ key: key, values: [val] } }
                    %{ key: currKey } -> 
                        if comparator.(key, currKey) do
                            if key == currKey do
                                %{ nod | state: %{ nod.state | values: nod.state.values ++ [val] } }
                            else
                                %{ nod | state: %{ key: key, values: [val] } }
                            end
                        else
                            nod
                        end
                end
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state.values)
                onCompleted(nod)
                nod
            end
        }, fn _ -> %Sentinel{} end) 
    end

    def maxBy(%MockDNode{id: id}, extractor) do
        _extremumBy(id, extractor, fn v1, v2 -> v1>=v2 end)
    end

    def minBy(%MockDNode{id: id}, extractor) do
        _extremumBy(id, extractor, fn v1, v2 -> v1<=v2 end)
    end

    def average(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                %{ nod | state: %{ nod.state | count: nod.state.count+1, sum: nod.state.sum+val } }
            end,
            onCompleted: fn nod, _, _ ->
                if nod.state.count==0 do
                    onNext(nod, 0)
                else
                    onNext(nod, nod.state.sum/nod.state.count)
                end
                onCompleted(nod)
                nod
            end
        }, fn _ -> %{ count: 0, sum: 0 } end)
    end

    def count(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                if condition.(val) do
                    %{ nod | state: nod.state+1 }
                else
                    nod
                end
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> 0 end)
    end

    def sum(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                %{ nod | state: nod.state+val }
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> 0 end)
    end

    def concat(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val, _ ->
                if nod.state.done do
                    onNext(nod, val)
                    nod
                else
                    case sender do
                        ^id1 -> 
                            onNext(nod, val)
                            nod
                        ^id2 -> %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val] } }
                    end
                end
            end,
            onCompleted: fn nod, sender, _ ->
                case sender do
                    ^id1 -> 
                        Enum.map(nod.state.buff, &onNext(nod, &1))
                        %{ nod | state: %{ nod.state | done: true } }
                    ^id2 ->
                        onCompleted(nod)
                        nod
                end
            end
        }, fn _ -> %{ buff: [], done: false } end)
    end

    def groupBy(%MockDNode{id: id}, extractKey, extractVal \\ nil) do
        res = Drepel.Env.createMidNode([id], fn nod, _, val, _ ->
            key = extractKey.(val)
            case extractVal do
                nil -> onNext(nod, {key, key})
                _ -> onNext(nod, {key, extractVal.(val)})
            end
            nod
        end)
        %MockGroup{id: res.id}
    end

    def subscribe(%MockGroup{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn nod, _, {key, val}, _ -> 
            if Map.has_key?(nod.state, key) do
                onNext(nod, Map.get(nod.state, key), val)
                nod
            else
                oldChildren = Drepel.Env.getNode(nod.id).children
                fct.(%MockDNode{id: nod.id}, key)
                newChildren = Drepel.Env.getNode(nod.id).children -- oldChildren
                nodes = Drepel.Env.startAllNodes()
                nod = Drepel.Env.updateAllChildrenFromNode(nodes, nod)
                onNext(nod, newChildren, val)
                %{ nod | state: Map.put(nod.state, key, newChildren) }
            end
        end, fn _ -> %{} end)
        nil
    end

    def toArray(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                %{ nod | state: nod.state ++ [val] }
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end,
            onError: fn nod, _, err, _ ->
                onNext(nod, nod.state)
                onError(nod, err)
                nod
            end
        }, fn _ -> [] end)
    end

    def defaultToMapFct(el), do: el

    def toMap(%MockDNode{id: id}, keyFct \\ &defaultToMapFct/1) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val, _ ->
                %{ nod | state: Map.put(nod.state, keyFct.(val), val) }
            end,
            onCompleted: fn nod, _, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end,
            onError: fn nod, _, err, _ ->
                onNext(nod, nod.state)
                onError(nod, err)
                nod
            end
        }, fn _ -> %{} end)
    end

    # SINK NODES

    def subscribe(%MockDNode{id: id}, nextFct, errFct \\ &Drepel.doNothing/1, complFct \\ &Drepel.doNothing/0) do
        Drepel.Env.createSink([id], %{ onNextSink: nextFct, onErrorSink: errFct, onCompletedSink: complFct })
    end

    def subscribeOnNext(%MockDNode{}=dnode, nextFct) do
        subscribe(dnode, nextFct)
    end

    def subscribeOnError(%MockDNode{}=dnode, errFct) do
        subscribe(dnode, &Drepel.doNothing/1, errFct)
    end

    def subscribeOnCompleted(%MockDNode{}=dnode, complFct) do
        subscribe(dnode, &Drepel.doNothing/1, &Drepel.doNothing/1, complFct)
    end


end

