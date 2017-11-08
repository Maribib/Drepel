defmodule DrepelTest do
    use ExUnit.Case
    use Drepel
    doctest Drepel

    def collect(stream, val \\ nil) do
        Agent.update(__MODULE__, &( &1 ++ [{ stream, val }] ))
    end

    def collected do
        Agent.get(__MODULE__, &(&1))
    end

    def collector(parent) do
        parent |> subscribe(&DrepelTest.collect(:next, &1), &DrepelTest.collect(:err, &1), fn -> DrepelTest.collect(:compl) end)
    end

    setup_all do 
        Agent.start_link(fn -> [] end, name: unquote(__MODULE__))
        :ok
    end

    setup do
        Agent.update(__MODULE__, fn _ -> [] end)
        use Drepel
        :ok
    end

    test "create 1source-1sub" do
        create(fn nod ->
            onNext(nod, 42)
            onNext(nod, 41)
            onCompleted(nod)
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:next, 41}, {:compl, nil}]
    end

    test "create 1source-2sub" do
        subFct = fn tag ->
            [ fn val -> 
                Agent.update(__MODULE__, &( &1 ++ [{ :next, tag, val }] ))
            end, 
            fn err -> 
                Agent.update(__MODULE__, &( &1 ++ [{ :err, tag, err }] ))
            end, 
            fn ->
                Agent.update(__MODULE__, &( &1 ++ [{ :compl, tag, nil }] ))
            end ]
        end

        n = create(fn nod ->
            onNext(nod, 42)
            onNext(nod, 41)
            onCompleted(nod)
        end)
        apply(&subscribe/4, [n] ++ subFct.(:c1))
        apply(&subscribe/4, [n] ++ subFct.(:c2))
        Drepel.run()
        res = collected()
        assert Enum.filter(res, fn {_stream, tag, _val} -> tag==:c1 end)==[{:next, :c1, 42}, {:next, :c1, 41}, {:compl, :c1, nil}]
        assert Enum.filter(res, fn {_stream, tag, _val} -> tag==:c2 end)==[{:next, :c2, 42}, {:next, :c2, 41}, {:compl, :c2, nil}]
    end

    test "just" do
        just("plop")
        |> collector
        Drepel.run()
        assert collected()==[{:next, "plop"}, {:compl, nil}]
    end

    test "start" do
        start(fn -> "res" end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, "res"}, {:compl, nil}]
    end

    test "repeat" do
        repeat(100, 3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 100}, {:next, 100}, {:next, 100}, {:compl, nil}]
    end

    test "range_1" do
        range(1..3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "range_2" do
        range(1, 3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "from_1" do
        from(["a", "b", "c"])
        |> collector
        Drepel.run()
        assert collected()==[{:next, "a"}, {:next, "b"}, {:next, "c"}, {:compl, nil}]
    end

    test "from_2" do
        from("bar")
        |> collector
        Drepel.run()
        assert collected()==[{:next, "b"}, {:next, "a"}, {:next, "r"}, {:compl, nil}]
    end

    test "from_3" do
        from(1, 2, 3, 4)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 3}, {:next, 4}, {:compl, nil}]
    end

    test "timer" do
        timer(200, 42)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:compl, nil}]
    end

    test "interval" do
        interval(50)
        |> collector
        Drepel.run(200)
        case length(collected()) do
            3 -> assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}]
            4 -> assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}, {:next, 3}]
        end
    end

    test "map" do
        range(0..3)
        |> map(fn el -> el*10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 10}, {:next, 20}, {:next, 30}, {:compl, nil}]
    end

    test "flatmap" do
        range(1, 2)
        |> flatmap(fn el ->
            range(el, 2)
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 2}, {:compl, nil}]
    end

    test "scan" do
        range(1..4)
        |> scan(fn val, acc -> val+acc end, 0)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 3}, {:next, 6}, {:next, 10}, {:compl, nil}]
    end
    
    test "buffer" do
        interval(25)
        |> buffer(interval(100))
        |> take(2)
        |> collector
        Drepel.run(250)
        assert collected()==[{:next, [0, 1, 2, 3]}, {:next, [4, 5, 6, 7]}]
    end

    test "bufferBoundaries" do
        interval(25)
        |> bufferBoundaries(interval(100))
        |> take(2)
        |> collector
        Drepel.run(350)
        assert length(collected())==2
        Enum.map(collected(), fn {:next, buff} -> assert length(buff)==4 end)
    end

    # TODO bufferSwitch

    # TODO bufferWithCount

    test "delay" do
        from(10)
        |> delay(200)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 10}, {:compl, nil}]
    end

    test "reduce" do
        from([1, 2, 3, 4, 5])
        |> reduce(fn v, acc -> v+acc end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 15}, {:compl, nil}]
    end

    test "skip" do
        range(0..4)
        |> skip(3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 3}, {:next, 4}, {:compl, nil}]
    end

    test "skipLast" do
        range(1..4)
        |> skipLast(2)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:compl, nil}]
    end

    test "ignoreElements" do
        range(0..4)
        |> ignoreElements()
        |> collector
        Drepel.run()
        assert collected()==[{:compl, nil}]
    end

    test "sample" do
        interval(30)
        |> sample(100)
        |> collector
        Drepel.run(205)
        assert collected()==[{:next, 2}, {:next, 5}]
    end

    test "elementAt_1" do
        range(0..4)
        |> elementAt(2)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 2}, {:compl, nil}]
    end

    test "elementAt_2" do
        range(0..1)
        |> elementAt(2)
        |> collector
        Drepel.run()
        assert collected()==[{:err, "Argument out of range"}]
    end

    test "distinct" do
        from([1, 2, 2, 1, 3])
        |> distinct()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "filter" do
        from([2, 30, 22, 5, 60, 1])
        |> filter(fn el -> el>10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 30}, {:next, 22}, {:next, 60}, {:compl, nil}]
    end

    test "first_1" do
        from([2, 30, 22, 5, 60, 1])
        |> first()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 2}, {:compl, nil}]
    end

    test "first_2" do
        from([2, 30, 22, 5, 60, 1])
        |> first(fn el -> el>10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 30}, {:compl, nil}]
    end

    test "first_3" do
        from([2, 30, 22, 5, 60, 1])
        |> first(fn el -> el>100 end)
        |> collector
        Drepel.run()
        assert collected()==[{:err, "Any element match condition."}]
    end

    test "last_1" do
        from([2, 30, 22, 5, 60, 1])
        |> last()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:compl, nil}]
    end

    test "last_2" do
        from([2, 30, 22, 5, 60, 1])
        |> last(fn el -> el>10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 60}, {:compl, nil}]
    end

    test "last_3" do
        from([2, 5, 1])
        |> last(fn el -> el>10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:err, "Any element match condition."}]
    end

    test "debounce" do
        from([
            %{value: 0, time: 100},
            %{value: 1, time: 600},
            %{value: 2, time: 400},
            %{value: 3, time: 700},
            %{value: 4, time: 200}
        ])
        |> flatmap(fn item ->
            from(item.value)
            |> delay(item.time)
        end)
        |> debounce(500)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 2}, {:next, 4}, {:compl, nil}]
    end

    test "take" do
        range(0, 5)
        |> take(3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}, {:compl, nil}]
    end

    test "takeLast" do
        range(0, 5)
        |> takeLast(3)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 3}, {:next, 4}, {:next, 5}, {:compl, nil}]
    end

    test "merge_1" do
        n1 = from([
            %{value: 20, time: 30},
            %{value: 40, time: 30},
            %{value: 60, time: 30},
            %{value: 80, time: 30},
            %{value: 100, time: 30}
        ]) |> flatmap(fn item ->
            from(item.value)
            |> delay(item.time)
        end)
        n2 = from([
            %{value: 1, time: 105},
            %{value: 1, time: 160}
        ])
        |> flatmap(fn item ->
            from(item.value)
            |> delay(item.time)
        end)

        merge(n1, n2)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 20}, {:next, 40}, {:next, 60}, {:next, 1}, {:next, 80}, {:next, 100}, {:next, 1}, {:compl, nil}]
    end

    test "merge_2" do
        n1 = Drepel.throw("err1")
        |> delay(25)
        n2 = Drepel.throw("err2")
        |> delay(50)
        n3 = just(42)

        merge([n1, n2, n3])
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:err, ["err1", "err2"]}]
    end

    test "zip" do
        n1 = from([
            %{time: 0, value: 1},
            %{time: 30, value: 2},
            %{time: 60, value: 3},
            %{time: 5, value: 4},
            %{time: 10, value: 5}
        ]) |> flatmap(fn el ->
            from(el.value) |> delay(el.time)
        end)
        n2= from([
            %{time: 15, value: "A"},
            %{time: 20, value: "B"},
            %{time: 30, value: "C"},
            %{time: 3, value: "D"}
        ]) |> flatmap(fn el ->
            from(el.value) |> delay(el.time)
        end)
        zip(n1, n2, fn v1, v2 ->
            "#{v1}#{v2}"
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, "1A"}, {:next, "2B"}, {:next, "3C"}, {:next, "4D"}, {:compl, nil}]
    end

    test "catch_1" do
        create(fn nod ->
            onNext(nod, 42)
            onError(nod, "err")
        end)
        |> dcatch(fn -> 
            just(3) 
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:next, 3}, {:compl, nil}]
    end

    test "catch_2" do
        create(fn nod ->
            onNext(nod, 42)
            onError(nod, "err")
        end)
        |> dcatch(fn ->
            Drepel.throw("err2")
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:err, "err2"}]
    end

    test "retry_1" do
        Agent.start_link(fn -> [{&onNext/2, [42]}, {&onError/2, ["err"]}, {&onNext/2, [42]}, {&onCompleted/1, []}, ] end, name: :retry)
        create(fn nod ->
            actions = Agent.get_and_update(:retry, fn state ->
                { Enum.slice(state, 0..1), Enum.slice(state, 2..length(state)) }
            end)
            Enum.map(actions, fn {fct, args} -> apply(fct, [nod]++args) end)
        end)
        |> retry()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:next, 42}, {:compl, nil}]
    end

    test "retry_2" do
        Drepel.throw("err")
        |> retry(3)
        |> collector
        Drepel.run()
        assert collected()==[{:err, "err"}]
    end

    test "combineLatest" do
        combineLatest(interval(50) |> delay(25), interval(50), fn v1, v2 -> "#{v1} #{v2}" end)
        |> take(7)
        |> collector
        Drepel.run(300)
        assert collected()==[{:next, "0 0"}, {:next, "0 1"}, {:next, "1 1"}, {:next, "1 2"}, {:next, "2 2"}, {:next, "2 3"}, {:next, "3 3"}, {:compl, nil}]
    end

    test "startWith" do
        from([1, 2, 3])
        |> startWith(0)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "every" do
        range(1..5)
        |> every(fn el -> el<10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, true}, {:compl, nil}]
    end

    test "amb" do
        amb([from([1,2,3]), from([4,5,6]) |> delay(50), from([7,8,9]) |> delay(100)])
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "contains_1" do
        range(1..10)
        |> contains(8)
        |> collector
        Drepel.run()
        assert collected()==[{:next, true}, {:compl, nil}]
    end

    test "contains_2" do
        range(1..10)
        |> contains(30)
        |> collector
        Drepel.run()
        assert collected()==[{:next, false}, {:compl, nil}]
    end

    test "contains_3" do
        Drepel.throw("err")
        |> contains(8)
        |> collector
        Drepel.run()
        assert collected()==[{:next, false}, {:err, "err"}]
    end

    test "contains_4" do
        from([1,2,2,3])
        |> contains(2)
        |> collector
        Drepel.run()
        assert collected()==[{:next, true}, {:compl, nil}]
    end

    test "defaultIfEmpty_1" do
        range(1..2)
        |> defaultIfEmpty(42)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:compl, nil}]
    end

    test "defaultIfEmpty_2" do
        empty()
        |> delay(100)
        |> defaultIfEmpty(42)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:compl, nil}]
    end

    test "sequenceEqual_1" do
        sequenceEqual([range(1..3), range(1..3) |> delay(50), range(1..3) |> delay(100)])
        |> collector
        Drepel.run()
        assert collected()==[{:next, true}, {:compl, nil}]
    end

    test "sequenceEqual_2" do
        sequenceEqual([range(1..3), range(1..3) |> delay(50), range(1..4) |> delay(100)])
        |> collector
        Drepel.run()
        assert collected()==[{:next, false}, {:compl, nil}]
    end

    test "sequenceEqual_3" do
        sequenceEqual([range(1..3), range(2..4) |> delay(50), range(1..3) |> delay(100)])
        |> collector
        Drepel.run()
        assert collected()==[{:next, false}, {:compl, nil}]
    end

    test "skipUntil" do
        interval(50)
        |> skipUntil(timer(120))
        |> collector
        Drepel.run(280)
        assert collected()==[{:next, 2}, {:next, 3}, {:next, 4}]
    end

    test "skipWhile" do
        range(1..5)
        |> skipWhile(fn el -> el != 3 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 3}, {:next, 4}, {:next, 5}, {:compl, nil}]
    end

    test "takeUntil_1" do
        interval(50)
        |> takeUntil(timer(120))
        |> collector
        Drepel.run(280)
        assert collected()==[{:next, 0}, {:next, 1}, {:compl, nil}]
    end

    test "takeUntil_2" do
        empty()
        |> takeUntil(timer(100, 42))
        |> collector
        Drepel.run()
        assert collected()==[{:compl, nil}]
    end

    test "takeWhile_1" do
        range(1..5)
        |> takeWhile(fn el -> el != 3 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 2}, {:compl, nil}]
    end

    test "takeWhile_2" do
        empty()
        |> takeWhile(fn el -> el != 3 end)
        |> collector
        Drepel.run()
        assert collected()==[{:compl, nil}]
    end

    test "max" do
        from([1, 2, 3])
        |> max()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 3}, {:compl, nil}]
    end

    test "min" do
        from([1, 2, 3])
        |> min()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:compl, nil}]
    end

    test "maxBy" do
        from([{"a", 1}, {"b", 2}, {"c", 3}, {"a", 3}])
        |> maxBy(fn {_, v} -> v end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, [{"c", 3}, {"a", 3}]}, {:compl, nil}]
    end

    test "minBy" do
        from([{"a", 1}, {"b", 2}, {"c", 3}, {"a", 3}, {"c", 1}])
        |> minBy(fn {_, v} -> v end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, [{"a", 1}, {"c", 1}]}, {:compl, nil}]
    end

    test "average" do
        range(1,3)
        |> average()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 2.0}, {:compl, nil}]
    end

    test "count" do
        from([2, 30, 22, 5, 60, 1])
        |> count(fn el -> el>10 end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 3}, {:compl, nil}]
    end

    test "sum" do
        from([1, 2, 3, 4, 5])
        |> sum()
        |> collector
        Drepel.run()
        assert collected()==[{:next, 15}, {:compl, nil}]
    end

     test "concat" do
        o1 = from([1, 1, 1])
        o2 = from([2, 2])
        concat(o1, o2)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 1}, {:next, 1}, {:next, 2}, {:next, 2}, {:compl, nil}]
    end

    test "groupBy" do
        from([{"a", 2}, {"b", 1}, {"c", 3}, {"a", 4}, {"b", 0}, {"c", 3} ])
        |> groupBy(fn {k, _} -> k end, fn {_, v} -> v end)
        |> subscribe(fn group, key ->
            group |> max() |> map(fn val -> {key, val} end) |> collector
        end)
        Drepel.run()
        res = collected()
        values = Enum.filter(res, fn {tag, _val} -> tag == :next end)
        m = Enum.reduce(values, %{}, fn {:next, {key, val}}, acc -> Map.put(acc, key, val) end)
        assert Map.get(m, "a")==4
        assert Map.get(m, "b")==1
        assert Map.get(m, "c")==3
        compls = Enum.filter(res, fn {tag, _val} -> tag == :compl end)
        assert length(compls)==3
    end
    

end
 