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

    test "range zero div error" do
        range(3..0) 
        |> map(&(6/&1))
        |> collector
        Drepel.run()
        assert collected()==[{:next, 2}, {:next, 3}, {:next, 6}, {:err, {:error, :badarith}}, {:compl, nil}]
    end

    test "flatmap" do
        range(1..3)
        |> flatmap(&Enum.to_list(1..&1))
        |> collector
        Drepel.run()
        assert collected()==[{:next, 1}, {:next, 1}, {:next, 2}, {:next, 1}, {:next, 2}, {:next, 3}, {:compl, nil}]
    end

    test "scan" do
        range(0..3) 
        |> scan(&(&1+&2))
        |> collector
        Drepel.run()
        assert collected()==[{:next, 0}, {:next, 1}, {:next, 3}, {:next, 6}, {:compl, nil}]
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

    test "bufferSwitch" do
        interval(25)
        |> bufferSwitch(interval(100))
        |> collector
        Drepel.run(300)
        assert collected()==[{:next, [3, 4, 5, 6]}]
    end

    test "bufferWithCount" do
        interval(25)
        |> bufferWithCount(2)
        |> collector
        Drepel.run(200)
        assert collected()==[{:next, [0, 1]}, {:next, [2, 3]}, {:next, [4, 5]}, {:next, [6, 7]}]
    end

end
