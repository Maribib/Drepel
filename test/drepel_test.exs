defmodule DrepelTest do
    use ExUnit.Case

    setup do
        Drepel.resetEnv()
    end

    @doc """
      s
     / \
    l1 l2
     \ / 
      t
    """
    test "single_node_1" do
        s = Drepel.miliseconds(100)
        l1 = Drepel.newSignal(s, fn s -> s+1 end)
        l2 = Drepel.newSignal(s, fn s -> s-1 end)
        Drepel.newSignal([l1, l2], fn v1, v2 -> 
            #IO.puts "#{inspect v1} #{inspect v2}"
            assert v1-v2==2
        end)
        Drepel.run(2000)
    end

    @doc """
      s1   s2
     / \   /
    l1 l2 /
     \ / /
      \ /
       t
    """
    test "single_node_2" do
        s1 = Drepel.miliseconds(100)
        s2 = Drepel.miliseconds(300)
        l1 = Drepel.newSignal(s1, fn s -> s+1 end)
        l2 = Drepel.newSignal(s1, fn s -> s-1 end)
        Drepel.newSignal([l1, l2, s2], fn v1, v2, v3 -> 
            #IO.puts "#{v1} #{v2} #{v3}"
            assert v1-v2==2
        end)
        Drepel.run(2000)
    end

    @doc """
        s1     s2
        / \   / \
       l1 l2 l3 l4
        \ /   \ /
         \__ __/
            t
    """
    test "single_node_3" do
        s1 = Drepel.miliseconds(100)
        s2 = Drepel.miliseconds(300)
        l1 = Drepel.newSignal(s1, fn s -> s+1 end)
        l2 = Drepel.newSignal(s1, fn s -> s-1 end)
        l3 = Drepel.newSignal(s2, fn s -> s+2 end)
        l4 = Drepel.newSignal(s2, fn s -> s-2 end)
        Drepel.newSignal([l1, l2, l3, l4], fn v1, v2, v3, v4 -> 
            #IO.puts "#{v1} #{v2} #{v3} #{v4}"
            assert v1-v2==2
            assert v3-v4==4
        end)
        Drepel.run(2000)
    end

     @doc """
        s1  s2  s3
        / \/  \/ \
       l1 l2  l3 l4
        \ /    \ /
         \__ __/
            t
    """
    test "single_node_4" do
        s1 = Drepel.miliseconds(100)
        s2 = Drepel.miliseconds(200)
        s3 = Drepel.miliseconds(300)
        l1 = Drepel.newSignal(s1, fn s -> s+1 end)
        l2 = Drepel.newSignal([s1, s2], fn v1, v2 -> v1+v2 end)
        l3 = Drepel.newSignal([s2, s3], fn v1, v2 -> v1-v2 end)
        l4 = Drepel.newSignal(s3, fn s -> s-2 end)
        Drepel.newSignal([l1, l2, l3, l4], fn v1, v2, v3, v4 -> 
            IO.puts "#{v1} #{v2} #{v3} #{v4}"
        end)
        Drepel.run(2000)
    end


    @doc """
      s
     / \
    l1  |
     \ / 
      t
    Before lauching the tests two VM are spawned to mock remote nodes.
    Their names are :"foo@127.0.0.1" and :"bar@127.0.0.1".
    Here a source runs on node :"foo@127.0.0.1", a signal on node :"bar@127.0.0.1"
    and signal or source without a node argument run on current node 
    which is the VM running the tests.
    """
    test "cluster_1" do
        s = Drepel.miliseconds(200, node: :"foo@127.0.0.1")
        l1 = Drepel.newSignal(s, &Distrib.inc/1, node: :"bar@127.0.0.1")
        Drepel.newSignal([s, l1], fn v1, v2 -> 
            assert v1+1==v2
        end)
        Drepel.run(2000)
    end

    test "cluster_2" do
        s = Drepel.miliseconds(200, node: :"foo@127.0.0.1")
        l1 = Drepel.scan(s, 0, &Distrib.count/2, node: :"bar@127.0.0.1")
        Drepel.newSignal([s, l1], fn v1, v2 -> 
            assert v1==v2
        end)
        Drepel.run(2000)
    end

    test "cluster_3" do
        s = Drepel.miliseconds(200, node: :"foo@127.0.0.1")
        l1 = Drepel.filter(s, 10, &Distrib.greaterThan10/1, node: :"bar@127.0.0.1")
        Drepel.newSignal(l1, fn v -> 
            assert v>=10
        end)
        Drepel.run(2000)
    end

    
end
 