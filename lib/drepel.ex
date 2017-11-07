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

defmodule Drepel do
    use Agent
    use Task

    defmacro __using__(_) do 
        quote do
            import unquote(__MODULE__)
            Env.new()
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

    def onNext(%DNode{id: id, children: children}, value) do 
        Enum.map(children, &DNode.onNext(&1, id, value))
    end

    def onNext(%DNode{id: id}, children, value) do
        Enum.map(children, &DNode.onNext(&1, id, value))
    end

    def onCompleted(%DNode{id: id, children: children}) do
        Enum.map(children, &DNode.onCompleted(&1, id))
        Manager.normalExit(self())
    end

    def onError(%DNode{id: id, children: children}, err) do
        Enum.map(children, &DNode.onError(&1, id, err))
        Manager.normalExit(self())
    end

    def create(fct) do
        Drepel.Env.createNode([:dnode_0], fct)
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
        res = create(%{ 
            onNext: fn nod -> nod end,
            onScheduled: fn nod, _name ->
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
            onScheduled: fn nod, _name ->
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
        Drepel.Env.createMidNode([id], fn nod, _, val ->
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
            onNext: fn nod, sender, val ->
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
            onCompleted: fn nod, sender ->
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
        Drepel.Env.createMidNode([id], fn nod, _, val ->
            newState = fct.(nod.state, val)
            onNext(nod, newState)
            %{ nod | state: newState }
        end, fn _ -> init end)
    end

    def _buffer(id, boundId, init, %{onNextVal: nextValFct, onNextBound: nextBoundFct}) do
        Drepel.Env.createMidNode([id, boundId], fn nod, sender, val ->
            case sender do
                ^id -> nextValFct.(nod, val)
                ^boundId -> nextBoundFct.(nod, val)
            end
        end, fn _ -> init end)
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
        Drepel.Env.createMidNode([id], fn nod, _, val ->
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
            onNext: fn nod, _, val ->
                Scheduler.schedule(nod, timespan, :onNext, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val], eid: nod.state.eid+1 } }
            end,
            onCompleted: fn nod, _ ->
                Scheduler.schedule(nod, timespan, :onCompleted, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | eid: nod.state.eid+1 } }
            end,
            onError: fn nod, _, err ->
                Scheduler.schedule(nod, timespan, :onError, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [err], eid: nod.state.eid+1 } }
            end,
            onScheduled: fn nod, name ->
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

    def reduce(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->  %{ nod | state: fct.(nod.state, val) } end,
            onCompleted: fn nod, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> init end)
    end

    def skip(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], fn nod, _, val ->  
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
            onNext: fn nod, _, val ->
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
            onNext: fn nod, _, val ->  
                if nod.state==0 do
                    onNext(nod, val)
                    onCompleted(nod)
                end
                %{ nod | state: nod.state-1 }
            end,
            onCompleted: fn nod, _ ->
                if nod.state>=0 do
                    onError(nod, "Argument out of range")
                end
                nod
            end
        }, fn _ -> pos end)
    end

    def _extractKey(v), do: v

    def distinct(%MockDNode{id: id}, extractKey \\ &_extractKey/1) do
        Drepel.Env.createMidNode([id], fn nod, _, val ->  
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
        Drepel.Env.createMidNode([id], fn nod, _, _val ->  
            nod
        end)
    end

    def sample(%MockDNode{id: id}, timespan) when is_integer(timespan) do
        res = Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                %{ nod | state: %{ nod.state | val: val } }
            end,
            onCompleted: fn nod, _ ->
                onCompleted(nod)
                %{ nod | state: %{ nod.state | compl: true } }
            end,
            onScheduled: fn nod, _name ->
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
        }, fn _ -> %{ val: %Sentinel{}, compl: false } end)
        EventCollector.schedule(res, timespan, nil, fn event ->
            %{ event | time: Timex.shift(event.time, microseconds: timespan*1000) } 
        end)
        res
    end

    def filter(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], fn nod, _, val ->  
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
            onNext: fn nod, _, val ->  
                if nod.state and condition.(val) do
                    onNext(nod, val)
                    onCompleted(nod)
                    %{ nod | state: false}
                else
                    nod
                end
            end,
            onCompleted: fn nod, _ ->
                if nod.state do
                    onError(nod, "Any element match condition.")
                end
                nod
            end
        }, fn _ -> true end)
    end

    def last(%MockDNode{id: id}, condition \\ &_trueCond/1) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                if condition.(val) do
                    %{ nod | state: val }
                else
                    nod
                end
            end,
            onCompleted: fn nod, _ ->
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
            onNext: fn nod, _, val -> 
                Scheduler.schedule(nod, timespan, :onNext, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | count: nod.state.count+1, val: val, eid: nod.state.eid+1 } }
            end,
            onCompleted: fn nod, _ ->
                Scheduler.schedule(nod, timespan, :onCompleted, nil, nod.state.eid)
                %{ nod | state: %{ nod.state | eid: nod.state.eid+1 } }
            end,
            onScheduled: fn nod, name ->
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
        }, fn _ -> %{ val: %Sentinel{}, count: 0, eid: 0 } end)
    end

    def take(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                if nod.state>0 do
                    onNext(nod, val)
                end
                if nod.state==0 do
                    onCompleted(nod)
                end
                %{ nod | state: nod.state-1 }
            end,
            onCompleted: fn nod, _ ->
                nod
            end
        }, fn _ -> nb end)  
    end

    def takeLast(%MockDNode{id: id}, nb) when is_integer(nb) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                if nod.state.size==nb do
                    [_ | tail] = nod.state.buff
                    %{ nod | state: %{ nod.state | buff: tail ++ [val] } }
                else
                    %{ nod | state: %{ nod.state | buff: nod.state.buff ++ [val], size: nod.state.size+1 } }
                end
            end,
            onCompleted: fn nod, _ ->
                Enum.map(nod.state.buff, &onNext(nod, &1))
                onCompleted(nod)
                nod
            end
        }, fn _ -> %{ buff: [], size: 0 } end)
    end

    def merge(dnodes) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, _, val ->
                onNext(nod, val)
                nod
            end,
            onCompleted: fn nod, sender ->
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
            onError: fn nod, sender, err ->
                compl = nod.state.compl ++ [sender]
                if length(nod.parents -- compl)==0 do
                    onError(nod, nod.state.errBuff ++ [err])
                end
                %{ nod | state: %{ nod.state | 
                    errBuff: nod.state.errBuff ++ [err], 
                    compl: compl} 
                }
            end
        }, fn _ -> %{ errBuff: [], compl: [] } end)
    end

    def merge(%MockDNode{}=dnode1, %MockDNode{}=dnode2) do
        merge([dnode1, dnode2])
    end

    def zip(dnodes, zipFct) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val ->
                aQueue = Map.get(nod.state.buffs, sender)
                isReady = :queue.len(aQueue)==0
                nod = %{ nod | state: %{ nod.state | buffs: %{ nod.state.buffs | sender => :queue.in(val, aQueue) }, ready: nod.state.ready+(isReady && 1 || 0) } }
                if nod.state.ready==nod.state.nbParents do
                    {ready, args, buffs, compl} = Enum.reduce(nod.state.buffs, {0, [], %{}, false}, fn {nodeId, aQueue}, {ready, args, buffs, compl} ->
                        {{:value, val}, aQueue} = :queue.out(aQueue)
                        isEmpty = :queue.is_empty(aQueue)
                        { ready+(isEmpty && 0 || 1), args ++ [val], Map.put(buffs, nodeId, aQueue), compl or (isEmpty and Map.get(nod.state.compls, nodeId)) }
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
            onCompleted: fn nod, sender ->
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
            onError: fn nod, sender, err ->
                if !nod.state.compl do
                    if :queue.is_empty(Map.get(nod.state.buffs, sender)) do
                        onError(nod, nod.state.errBuff ++ [err])
                        %{ nod | state: %{ nod.state | compl: true } }
                    else
                        %{ nod | state: %{ nod.state | compls: %{ nod.state.compls | sender => true }, errBuff: nod.state.errBuff ++ [err] } }
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

    def combineLatest(dnodes, combineFct) when is_list(dnodes) do
        Drepel.Env.createMidNode(Enum.map(dnodes, fn %MockDNode{id: id} -> id end), %{
            onNext: fn nod, sender, val ->
                state = %{ nod.state | vals: %{ nod.state.vals | sender => val }, ready: nod.state.ready-(Map.get(nod.state.vals, sender)==%Sentinel{} && 1 || 0) }
                if (state.ready==0) do
                    onNext(nod, apply(combineFct, Enum.map(nod.parents, fn id -> Map.get(state.vals, id) end)))
                end
                %{ nod | state: state }
            end,
            onCompleted: fn nod, sender ->
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
                    %{ nod | state: %{ nod.state | compls: %{ nod.state.compls | sender => true }, compl: nod.state.compl+1 } }
                end
            end,
            onError: fn nod, sender, err ->
                if Map.get(nod.state.compls, sender) do
                    nod
                else
                    if (nod.state.compl==1) do
                        onError(nod, nod.state.errBuff ++ [err])
                    end
                    %{ nod | state: %{ nod.state | compls: %{ nod.state.compls | sender => true }, compl: nod.state.compl+1, errBuff: nod.state.errBuff ++ [err] } }
                end
            end
        }, fn nod -> %{
            errBuff: [],
            ready: length(nod.parents), 
            compl: length(nod.parents), 
            vals: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, %Sentinel{}) end),
            compls: Enum.reduce(nod.parents, %{}, fn id, acc -> Map.put(acc, id, false) end)
        } end)
    end

    def combineLatest(%MockDNode{}=dnode1, %MockDNode{}=dnode2, combineFct) do
        combineLatest([dnode1, dnode2], combineFct)
    end

    def startWith(%MockDNode{id: id}, v1, v2 \\ nil, v3 \\ nil, v4 \\ nil, v5 \\ nil, v6 \\ nil, v7 \\ nil, v8 \\ nil, v9 \\ nil) do
        values = Enum.to_list(binding() |> tl() |> Stream.filter(fn {_, b} -> !is_nil(b) end) |> Stream.map(fn {_, b} -> b end))
        res = Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val -> 
                #IO.puts "startWith onNext"
                if nod.state.done do
                    onNext(nod, val)
                    nod
                else
                    %{ nod | state: %{ nod.state | values: nod.state.values ++ [val] } }
                end
            end,
            onError: fn nod, _, err ->
                #IO.puts "startWith onError"
                Enum.map(nod.state.values, &onNext(nod, &1))
                onError(nod, err)
                %{ nod | state: %{ nod.state | done: true, values: [] } }
            end,
            onCompleted: fn nod, _ ->
                #IO.puts "startWith onCompleted"
                Enum.map(nod.state.values, &onNext(nod, &1))
                onCompleted(nod)
                %{ nod | state: %{ nod.state | done: true, values: [] } }
            end,
            onScheduled: fn nod, _ -> 
                #IO.puts "startWith onScheduled"
                #IO.puts inspect nod.state.values
                Enum.map(nod.state.values, &onNext(nod, &1))
                %{ nod | state: %{ nod.state | done: true, values: [] } }
            end
        }, fn _ -> %{ values: values, done: false } end)
        EventCollector.schedule(res, 0)
        res
    end

    def _extremum(id, comparator) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
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
            onCompleted: fn nod, _ ->
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
            onNext: fn nod, _, val ->
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
            onCompleted: fn nod, _ ->
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
            onNext: fn nod, _, val ->
                %{ nod | state: %{ nod.state | count: nod.state.count+1, sum: nod.state.sum+val } }
            end,
            onCompleted: fn nod, _ ->
                if nod.state.count==0 do
                    onNext(nod, 0)
                else
                    onNext(nod, div(nod.state.sum, nod.state.count))
                end
                onCompleted(nod)
                nod
            end
        }, fn _ -> %{ count: 0, sum: 0 } end)
    end

    def count(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                if condition.(val) do
                    %{ nod | state: nod.state+1 }
                else
                    nod
                end
            end,
            onCompleted: fn nod, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> 0 end)
    end

    def sum(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn nod, _, val ->
                %{ nod | state: nod.state+val }
            end,
            onCompleted: fn nod, _ ->
                onNext(nod, nod.state)
                onCompleted(nod)
                nod
            end
        }, fn _ -> 0 end)
    end

    def concat(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn nod, sender, val ->
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
            onCompleted: fn nod, sender ->
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
        res = Drepel.Env.createMidNode([id], fn nod, _, val ->
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
        Drepel.Env.createMidNode([id], fn nod, _, {key, val} -> 
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
                res = %{ nod | state: Map.put(nod.state, key, newChildren) }
                res
            end
        end, fn _ -> %{} end)
        nil
    end

    # SINK NODES

    def subscribe(%MockDNode{id: id}, nextFct, errFct \\ &Drepel.doNothing/1, complFct \\ &Drepel.doNothing/0) do
        Drepel.Env.createSink([id], %{ onNextSink: nextFct, onErrorSink: errFct, onCompletedSink: complFct })
    end

end

