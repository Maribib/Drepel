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

    test "interval |> map" do
        interval(50) 
        |> map(fn el -> el*10 end)
        |> collector
        Drepel.run(200)
        assert collected()==[{:next, 0}, {:next, 10}, {:next, 20}, {:next, 30}]
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

    test "groupBy" do
        IO.puts "groupby"
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
 