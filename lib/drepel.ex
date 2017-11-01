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
        create(fn obs -> 
            onCompleted(obs)
        end) 
    end

    def never do
        create(fn _ -> nil end)
    end

    def throw(err) do
        create(fn obs -> 
            onError(obs, err)
        end)
    end

    def just(value) do
        create(fn obs ->
            onNext(obs, value)
            onCompleted(obs)
        end)
    end

    def start(fct) do
        create(fn obs ->
            onNext(obs, fct.())
            onCompleted(obs)
        end)
    end

    def repeat(val, times) when is_integer(times) and times>=0 do
        create(fn obs ->
            case times do
               0 -> nil
               _ -> Enum.map(1..times, fn _ -> onNext(obs, val) end)
            end
            onCompleted(obs)
        end)
    end


    def _range(enum) do
        create(fn obs -> 
            Enum.map(enum, &onNext(obs, &1))
            onCompleted(obs)
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
            onNext: fn obs -> obs end,
            onScheduled: fn obs, _name ->
                onNext(obs, val)
                onCompleted(obs)
                obs
            end
        })
        EventCollector.schedule(res, offset)
        res
    end

    def interval(duration) do
        res = Drepel.Env.createNode([:dnode_0], %{
            onNext: fn obs -> obs end,
            onScheduled: fn obs, _name ->
                onNext(obs, obs.state)
                %{ obs | state: obs.state+1 }
            end
        }, fn -> 0 end)
        EventCollector.schedule(res, duration, nil, fn event -> 
            %{ event | time: Timex.shift(event.time, microseconds: duration*1000) } 
        end)
        res
    end

    # MID NODES

    def map(%MockDNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            onNext(obs, fct.(val))
            obs
        end)
    end

    def _flatmap(obs, fct, val, id) do
        subObs = fct.(val)
        case subObs do
            %MockDNode{id: subId} ->
                Drepel.Env.addTmpChild(subId, id)
                Drepel.Env.runWithAncestors(subId)
                %{ obs | parents: obs.parents ++ [subId] }
            _ -> 
                onError(obs, "Flatmap argument must be a function that returns a node.")
                obs
        end 
    end

    def flatmap(%MockDNode{id: id}, fct) do    
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, sender, val ->
                if sender==id do
                    obs = if length(obs.state.buff)==0 do
                        _flatmap(obs, fct, val, obs.id)
                    else
                        obs
                    end
                    %{ obs | state: %{ obs.state | buff: obs.state.buff ++ [val] } }
                else
                    onNext(obs, val)
                    obs
                end
            end,
            onCompleted: fn obs, sender ->
                if sender==id do
                    case length(obs.state.buff) do
                        0 ->
                            onCompleted(obs)
                            obs
                        _ -> %{ obs | state: %{ obs.state | compl: true } }
                    end
                else
                    Drepel.Env.removeWithAncestors(sender)
                    [head | tail] = obs.state.buff
                    obs = %{ obs | state: %{ obs.state | buff: tail } }
                    if length(tail)>0 do
                        [head | _tail] = tail
                        _flatmap(obs, fct, head, obs.id)
                    else
                        if obs.state.compl do
                            onCompleted(obs)
                        end
                        obs
                    end
                end
            end
        }, fn -> %{ compl: false, buff: [] } end)
    end

    def scan(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            newState = fct.(obs.state, val)
            onNext(obs, newState)
            %{ obs | state: newState }
        end, fn -> init end)
    end

    def _buffer(id, boundId, init, %{onNextVal: nextValFct, onNextBound: nextBoundFct}) do
        Drepel.Env.createMidNode([id, boundId], fn obs, sender, val ->
            case sender do
                ^id -> nextValFct.(obs, val)
                ^boundId -> nextBoundFct.(obs, val)
            end
        end, fn -> init end)
    end

    @doc """
    Implement bufferClosingSelector
    """
    def buffer(%MockDNode{id: id}, %MockDNode{id: boundId}) do
        _buffer(id, boundId, [], %{ 
            onNextVal: fn obs, val -> %{ obs | state: obs.state ++ [val] } end,
            onNextBound: fn obs, _ -> 
                onNext(obs, obs.state)
                %{ obs | state: [] }
            end
        })
    end

    def _ignoreWhenNil do 
        fn obs, val -> 
            case obs.state do
                nil -> obs
                _ -> %{ obs | state: obs.state ++ [val] }
            end
        end
    end

    def bufferBoundaries(%MockDNode{id: id}, %MockDNode{id: boundId}) do
        _buffer(id, boundId, nil, %{ 
            onNextVal: _ignoreWhenNil(),
            onNextBound: fn obs, _ -> 
                case obs.state do
                    nil -> nil
                    _ -> onNext(obs, obs.state)
                end
                %{ obs | state: [] }
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
            onNextBound: fn obs, _ ->
                case obs.state do
                    nil -> %{ obs | state: [] }
                    _ -> 
                        onNext(obs, obs.state)
                        %{ obs | state: nil }
                end
            end
        })
    end

    @initBuffWithCountState %{length: 0, buff: []}

    def bufferWithCount(%MockDNode{id: id}, count) do 
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            case obs.state.length+1 do
                ^count -> 
                    onNext(obs, obs.state.buff ++ [val])
                    %{ obs | state: @initBuffWithCountState }
                _ -> %{ obs | state: %{ obs.state | length: obs.state.length+1, buff: obs.state.buff ++ [val]} }
            end
        end, fn -> @initBuffWithCountState end)
    end

    def delay(%MockDNode{id: id}, timespan) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                Scheduler.schedule(obs, timespan, :onNext, nil, obs.state.eid)
                %{ obs | state: %{ obs.state | buff: obs.state.buff ++ [val], eid: obs.state.eid+1 } }
            end,
            onCompleted: fn obs, _ ->
                Scheduler.schedule(obs, timespan, :onCompleted, nil, obs.state.eid)
                %{ obs | state: %{ obs.state | eid: obs.state.eid+1 } }
            end,
            onScheduled: fn obs, name ->
                case name do
                    :onNext ->
                        [head | tail] = obs.state.buff
                        onNext(obs, head)
                        %{ obs | state: %{ obs.state | buff: tail } }
                    :onCompleted -> 
                        onCompleted(obs)
                        obs
                end
            end
        }, fn -> %{ buff: [], eid: 0 } end)
    end

    def reduce(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->  %{ obs | state: fct.(obs.state, val) } end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
                obs
            end
        }, fn -> init end)
    end

    def filter(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->  
            if condition.(val) do
                onNext(obs, val)
            end
            obs
        end)
    end

    def _trueCond(_val) do
        true
    end

    def first(%MockDNode{id: id}, condition \\ &_trueCond/1) do
        Drepel.Env.createMidNode([id], %{ 
            onNext: fn obs, _, val ->  
                if obs.state and condition.(val) do
                    onNext(obs, val)
                    onCompleted(obs)
                    %{ obs | state: false}
                else
                    obs
                end
            end,
            onCompleted: fn obs, _ ->
                if obs.state do
                    onError(obs, "Any element match condition.")
                end
                obs
            end
        }, fn -> true end)
    end

    def last(%MockDNode{id: id}, condition \\ &_trueCond/1) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                if condition.(val) do
                    %{ obs | state: val }
                else
                    obs
                end
            end,
            onCompleted: fn obs, _ ->
                case obs.state do
                    %Sentinel{} -> onError(obs, "Any element match condition.")
                    _ -> 
                        onNext(obs, obs.state)
                        onCompleted(obs)
                end
                obs
            end
        }, fn -> %Sentinel{} end)
    end

    def debounce(%MockDNode{id: id}, timespan) do
        Drepel.Env.createMidNode([id], %{ 
            onNext: fn obs, _, val -> 
                Scheduler.schedule(obs, timespan, :onNext, nil, obs.state.eid)
                %{ obs | state: %{ obs.state | count: obs.state.count+1, val: val, eid: obs.state.eid+1 } }
            end,
            onCompleted: fn obs, _ ->
                Scheduler.schedule(obs, timespan, :onCompleted, nil, obs.state.eid)
                %{ obs | state: %{ obs.state | eid: obs.state.eid+1 } }
            end,
            onScheduled: fn obs, name ->
                case name do
                    :onNext ->
                        if obs.state.count==1 do
                            onNext(obs, obs.state.val)
                        end
                        %{ obs | state: %{ obs.state | count: obs.state.count-1 } }
                    :onCompleted ->
                        onCompleted(obs)
                        obs
                end
            end
        }, fn -> %{ val: %Sentinel{}, count: 0, eid: 0 } end)
    end

    def _extremum(id, comparator) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                case obs.state do
                    %Sentinel{} -> %{ obs | state: val }
                    _ -> 
                        if comparator.(val, obs.state) do
                            %{ obs | state: val }
                        else
                            obs
                        end
                end
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
                obs
            end
        }, fn -> %Sentinel{} end)  
    end

    def max(%MockDNode{id: id}) do
        _extremum(id, fn v1, v2 -> v1>v2 end)
    end

    def min(%MockDNode{id: id}) do
        _extremum(id, fn v1, v2 -> v1<v2 end)
    end

    def _extremumBy(id, extractor, comparator) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                key = extractor.(val)
                case obs.state do
                    %Sentinel{} -> %{ obs | state: %{ key: key, values: [val] } }
                    %{ key: currKey } -> 
                        if comparator.(key, currKey) do
                            if key == currKey do
                                %{ obs | state: %{ obs.state | values: obs.state.values ++ [val] } }
                            else
                                %{ obs | state: %{ key: key, values: [val] } }
                            end
                        else
                            obs
                        end
                end
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state.values)
                onCompleted(obs)
                obs
            end
        }, fn -> %Sentinel{} end) 
    end

    def maxBy(%MockDNode{id: id}, extractor) do
        _extremumBy(id, extractor, fn v1, v2 -> v1>=v2 end)
    end

    def minBy(%MockDNode{id: id}, extractor) do
        _extremumBy(id, extractor, fn v1, v2 -> v1<=v2 end)
    end

    def average(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                %{ obs | state: %{ obs.state | count: obs.state.count+1, sum: obs.state.sum+val } }
            end,
            onCompleted: fn obs, _ ->
                if obs.state.count==0 do
                    onNext(obs, 0)
                else
                    onNext(obs, div(obs.state.sum, obs.state.count))
                end
                onCompleted(obs)
                obs
            end
        }, fn -> %{ count: 0, sum: 0 } end)
    end

    def count(%MockDNode{id: id}, condition) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                if condition.(val) do
                    %{ obs | state: obs.state+1 }
                else
                    obs
                end
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
                obs
            end
        }, fn -> 0 end)
    end

    def sum(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                %{ obs | state: obs.state+val }
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
                obs
            end
        }, fn -> 0 end)
    end

    def concat(%MockDNode{id: id1}, %MockDNode{id: id2}) do
        Drepel.Env.createMidNode([id1, id2], %{
            onNext: fn obs, sender, val ->
                if obs.state.done do
                    onNext(obs, val)
                    obs
                else
                    case sender do
                        ^id1 -> 
                            onNext(obs, val)
                            obs
                        ^id2 -> %{ obs | state: %{ obs.state | buff: obs.state.buff ++ [val] } }
                    end
                end
            end,
            onCompleted: fn obs, sender ->
                case sender do
                    ^id1 -> 
                        Enum.map(obs.state.buff, &onNext(obs, &1))
                        %{ obs | state: %{ obs.state | done: true } }
                    ^id2 ->
                        onCompleted(obs)
                        obs
                end
            end
        }, fn -> %{ buff: [], done: false } end)
    end

    def groupBy(%MockDNode{id: id}, extractKey, extractVal \\ nil) do
        res = Drepel.Env.createMidNode([id], fn obs, _, val ->
            key = extractKey.(val)
            case extractVal do
                nil -> onNext(obs, {key, key})
                _ -> onNext(obs, {key, extractVal.(val)})
            end
            obs
        end)
        %MockGroup{id: res.id}
    end

    def subscribe(%MockGroup{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, {key, val} -> 
            if Map.has_key?(obs.state, key) do
                onNext(obs, Map.get(obs.state, key), val)
                obs
            else
                oldChildren = Drepel.Env.getNode(obs.id).children
                fct.(%MockDNode{id: obs.id}, key)
                newChildren = Drepel.Env.getNode(obs.id).children -- oldChildren
                nodes = Drepel.Env.startAllNodes()
                obs = Drepel.Env.updateAllChildrenFromNode(nodes, obs)
                onNext(obs, newChildren, val)
                res = %{ obs | state: Map.put(obs.state, key, newChildren) }
                res
            end
        end, fn -> %{} end)
        nil
    end

    # SINK NODES

    def subscribe(%MockDNode{id: id}, nextFct, errFct \\ &Drepel.doNothing/1, complFct \\ &Drepel.doNothing/0) do
        Drepel.Env.createSink([id], %{ onNextSink: nextFct, onErrorSink: errFct, onCompletedSink: complFct })
    end

end

