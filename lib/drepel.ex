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
    end

    def onError(%DNode{id: id, children: children}, err) do
        Enum.map(children, &DNode.onError(&1, id, err))
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
        res = create(&doNothing/1)
        EventCollector.schedule(res, offset, fn obs ->
            onNext(obs, val)
            onCompleted(obs)
        end)
        res
    end

    def interval(duration) do
        res = create(&doNothing/1)
        EventCollector.schedule(res, duration, 
            fn obs -> onNext(obs, Agent.get_and_update({:global, obs.id}, fn s -> {s, s+1} end)) end, 
            fn event -> 
                %{ event | time: Timex.shift(event.time, microseconds: duration*1000) } 
            end
        )
        Agent.start_link(fn -> 0 end, name: {:global, res.id})
        res
    end

    # MID NODES

    def map(%MockDNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            onNext(obs, fct.(val))
            obs
        end)
    end

    def flatmap(%MockDNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            res = fct.(val)
            case Enumerable.impl_for res  do
                val when is_nil(val) -> onError(obs, "Function given to flatmap must return a type that implement Enumerable protocol")
                _ -> Enum.map(res, &onNext(obs, &1))
            end
            obs
        end)
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

    def bufferWithTime(timeSpan) do
    end

    def reduce(%MockDNode{id: id}, fct, init \\ 0) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->  %{ obs | state: fct.(obs.state, val) } end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
            end
        }, fn -> init end)
    end

    def max(%MockDNode{id: id}) do
        Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                case obs.state do
                    %Sentinel{} -> %{ obs | state: val }
                    _ -> 
                        if val>obs.state do
                            %{ obs | state: val }
                        else
                            obs
                        end
                end
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, obs.state)
                onCompleted(obs)
            end
        }, fn -> %Sentinel{} end)    
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

