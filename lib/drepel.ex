require Drepel.Env, as: Env
require DNode
require MockDNode
require Drepel.Supervisor

defmodule Drepel do
    use Agent
    use Task

    defmacro __using__(_) do 
        quote do
            import unquote(__MODULE__)
            Env.new()
            Scheduler.new()
        end
    end

    def doNothing(_ \\ nil) do
        nil
    end

    def run(duration \\ :inf) do
        Drepel.Supervisor.new()
        Drepel.Supervisor.join(duration)
    end

    def onNext(%MockDNode{id: id}, value) do
        env = Drepel.Env.get()
        node = Map.get(env.nodes, id)
        Enum.map(node.children, &_runNode(env, &1, node.id, :onNext, value))
    end

    def onCompleted(%MockDNode{id: id}) do
        env = Drepel.Env.get()
        node = Map.get(env.nodes, id)
        Enum.map(node.children, &_runNode(env, &1, node.id, :onCompleted))
    end

    def onError(%MockDNode{id: id}, err) do
        env = Drepel.Env.get()
        node = Map.get(env.nodes, id)
        Enum.map(node.children, &_runNode(env, &1, node.id, :onError, err))
    end

    def _runNode(env, id, sender, label, arg \\ nil) do
        node = Map.get(env.nodes, id)
        case { label, node.runFct} do
            # sink nodes (subscribe)
            {:onNext, %{onNextSink: nextFct}} -> nextFct.(arg)
            {:onError, %{onErrorSink: errFct}} -> errFct.(arg)
            {:onCompleted, %{onCompletedSink: complFct}} -> complFct.()
            # mid nodes
            {:onNext, %{onNext: nextFct}} -> nextFct.(%MockDNode{id: id}, sender, arg)
            {:onError, %{onError: errFct}} -> errFct.(%MockDNode{id: id}, sender, arg)
            {:onCompleted, %{onCompleted: complFct}} -> complFct.(%MockDNode{id: id}, sender)
            {:onError, _} -> Enum.map(node.children, &_runNode(env, &1, id, :onError, arg)) # propagate
            {:onCompleted, _} -> Enum.map(node.children, &_runNode(env, &1, id, :onCompleted)) # propagate
        end
    end

    def create(fct) do
        Drepel.Env.createNode([0], fct)
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
        Scheduler.schedule(res, offset, fn obs ->
            onNext(obs, val)
            onCompleted(obs)
        end)
        res
    end

    def interval(duration) do
        res = create(&doNothing/1)
        Scheduler.schedule(res, duration, 
            fn obs -> onNext(obs, Agent.get_and_update({:global, obs.id}, fn s -> {s, s+1} end)) end, 
            fn event -> 
                %{ event | time: Timex.shift(event.time, microseconds: duration*1000) } 
            end
        )
        Agent.start_link(fn -> 0 end, name: {:global, res.id})
        res
    end

    # MID NODES

    def map(%DNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            onNext(obs, fct.(val))
        end)
    end

    def flatmap(%DNode{id: id}, fct) do
        Drepel.Env.createMidNode([id], fn obs, _, val ->
            res = fct.(val)
            case Enumerable.impl_for res  do
                val when is_nil(val) -> onError(obs, "Function given to flatmap must return a type that implement Enumerable protocol")
                _ -> Enum.map(res, &onNext(obs, &1))
            end
        end)
    end

    def scan(%DNode{id: id}, fct, init \\ 0) do
        res = Drepel.Env.createMidNode([id], fn obs, _, val ->
            Agent.update({:global, obs.id}, &fct.(&1, val))
            onNext(obs, Agent.get({:global, obs.id}, &(&1)))
        end)
        Agent.start_link(fn -> init end, name: {:global, res.id})
        res
    end

    def _buffer(id, boundId, init, %{onNextVal: nextValFct, onNextBound: nextBoundFct}) do
        res = Drepel.Env.createMidNode([id, boundId], fn obs, sender, val ->
            case sender do
                ^id -> nextValFct.(obs, val)
                ^boundId -> nextBoundFct.(obs, val)
            end
        end)
        Agent.start_link(fn -> init end, name: {:global, res.id})
        res
    end

    @doc """
    Implement bufferClosingSelector
    """
    def buffer(%DNode{id: id}, %DNode{id: boundId}) do
        _buffer(id, boundId, [], %{ 
            onNextVal: &Agent.update({:global, &1.id}, fn buff -> buff ++ [&2] end),
            onNextBound: fn obs, _ -> onNext(obs, Agent.get_and_update({:global, obs.id}, fn buff -> {buff, []} end))end
        })
    end

    def _ignoreWhenNil do 
        &Agent.update({:global, &1.id}, 
            fn buff -> 
                case buff do
                    nil -> nil
                    _ -> buff ++ [&2]
                end 
            end)
    end

    def bufferBoundaries(%DNode{id: id}, %DNode{id: boundId}) do
        _buffer(id, boundId, nil, %{ 
            onNextVal: _ignoreWhenNil(),
            onNextBound: fn obs, _ -> 
                case Agent.get_and_update({:global, obs.id}, &({&1, []})) do
                    nil -> nil
                    buff -> onNext(obs, buff)
                end 
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

    def bufferSwitch(%DNode{id: id}, %DNode{id: boundId}) do
        _buffer(id, boundId, nil, %{
            onNextVal: _ignoreWhenNil(),
            onNextBound: fn obs, _ ->
                case Agent.get_and_update({:global, obs.id}, _updateBufferSwitch()) do
                    nil -> nil
                    buff -> onNext(obs, buff)
                end 
            end
        })
    end

    @initBuffWithCountState %{length: 0, buff: []}

    def _updateBufferWithCountState(state, count, val) do
        case state.length+1 do
            ^count -> { state.buff ++ [val], @initBuffWithCountState } 
            _ -> {nil, %{ state | length: state.length+1, buff: state.buff ++ [val]}}
        end
    end

    def bufferWithCount(%DNode{id: id}, count) do 
        res = Drepel.Env.createMidNode([id], fn obs, _, val ->
            case Agent.get_and_update({:global, obs.id}, &_updateBufferWithCountState(&1, count, val)) do
                nil -> nil
                buff -> onNext(obs, buff)
            end
        end)
        Agent.start_link(fn -> @initBuffWithCountState end, name: {:global, res.id})
        res
    end

    def bufferWithTime(timeSpan) do
        
    end

    def reduce(%DNode{id: id}, fct, init \\ 0) do
        res = Drepel.Env.createMidNode([id], %{
            onNext: fn obs, _, val ->
                Agent.update({:global, obs.id}, &fct.(&1, val))
            end,
            onCompleted: fn obs, _ ->
                onNext(obs, Agent.get({:global, obs.id}, &(&1)))
                onCompleted(obs)
            end
        })
        Agent.start_link(fn -> init end, name: {:global, res.id})
        res
    end

    # SINK NODES

    def subscribe(%DNode{id: id}, nextFct, errFct \\ &Drepel.doNothing/1, complFct \\ &Drepel.doNothing/0) do
        Drepel.Env.createSink([id], %{ onNextSink: nextFct, onErrorSink: errFct, onCompletedSink: complFct })
    end

end

