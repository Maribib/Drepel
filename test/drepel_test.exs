defmodule DrepelTest do
    use ExUnit.Case

    @doc """
      s
     / \
    l1 l2
     \ / 
      t
    """
    test "single_node_1" do
        s = Drepel.Source.miliseconds(100)
        l1 = Drepel.Signal.new(s, fn s -> s+1 end)
        l2 = Drepel.Signal.new(s, fn s -> s-1 end)
        Drepel.Signal.new([l1, l2], fn v1, v2 -> 
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
        s1 = Drepel.Source.miliseconds(100)
        s2 = Drepel.Source.miliseconds(300)
        l1 = Drepel.Signal.new(s1, fn s -> s+1 end)
        l2 = Drepel.Signal.new(s1, fn s -> s-1 end)
        Drepel.Signal.new([l1, l2, s2], fn v1, v2, v3 -> 
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
        s1 = Drepel.Source.miliseconds(100)
        s2 = Drepel.Source.miliseconds(300)
        l1 = Drepel.Signal.new(s1, fn s -> s+1 end)
        l2 = Drepel.Signal.new(s1, fn s -> s-1 end)
        l3 = Drepel.Signal.new(s2, fn s -> s+2 end)
        l4 = Drepel.Signal.new(s2, fn s -> s-2 end)
        Drepel.Signal.new([l1, l2, l3, l4], fn v1, v2, v3, v4 -> 
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
        s1 = Drepel.Source.miliseconds(100)
        s2 = Drepel.Source.miliseconds(200)
        s3 = Drepel.Source.miliseconds(300)
        l1 = Drepel.Signal.new(s1, fn s -> s+1 end)
        l2 = Drepel.Signal.new([s1, s2], fn v1, v2 -> v1+v2 end)
        l3 = Drepel.Signal.new([s2, s3], fn v1, v2 -> v1-v2 end)
        l4 = Drepel.Signal.new(s3, fn s -> s-2 end)
        Drepel.Signal.new([l1, l2, l3, l4], fn v1, v2, v3, v4 -> 
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
    """
    test "cluster_1" do
        s = Drepel.Source.miliseconds(200, node: :"foo@127.0.0.1")
        l1 = Drepel.Signal.new(s, &Distrib.inc/1, node: :"bar@127.0.0.1")
        Drepel.Signal.new([s, l1], fn v1, v2 -> 
            assert v1+1==v2
        end)
        Drepel.run(2000)
    end

    
end
 