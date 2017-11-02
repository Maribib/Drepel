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
        create(fn obs ->
            onNext(obs, 42)
            onNext(obs, 41)
            onCompleted(obs)
        end)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:next, 41}, {:compl, nil}]
    end

    test "create 1source-2sub" do
        n = create(fn obs ->
            onNext(obs, 42)
            onNext(obs, 41)
            onCompleted(obs)
        end)
        n |> collector
        n |> collector
        Drepel.run()
        assert collected()==[{:next, 42}, {:next, 42}, {:next, 41}, {:next, 41}, {:compl, nil}, {:compl, nil}]
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
        assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}, {:next, 3}]
    end

    test "map" do
        interval(50) 
        |> map(fn el -> el*10 end)
        |> collector
        Drepel.run(200)
        assert collected()==[{:next, 0}, {:next, 10}, {:next, 20}, {:next, 30}]
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
        |> collector
        Drepel.run(200)
        assert collected()==[{:next, [0, 1, 2, 3]}, {:next, [4, 5, 6, 7]}]
    end

    test "bufferBoundaries" do
        interval(25)
        |> bufferBoundaries(interval(100))
        |> collector
        Drepel.run(300)
        assert collected()==[{:next, [3, 4, 5, 6]}, {:next, [7, 8, 9, 10]}]
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

    test "startWith" do
        from([1, 2, 3])
        |> startWith(0)
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
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
 