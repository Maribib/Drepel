
defmodule DNode do
    @enforce_keys [:id, :fct, :onReceive, :args, :buffs]
    defstruct [ :id, :fct, :onReceive, :args, :dependencies, :buffs,
    parents: [], children: [], startReceived: 0 ]

    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, aDNode) do
        {id, _node} = aDNode.id
        GenServer.start_link(__MODULE__, aDNode, name: id)
    end

    def propagate(id, source, sender, value) do
        GenServer.cast(id, {:propagate, source, sender, value})
    end

    def propagateDefault(id, sender, value) do
        GenServer.cast(id, {:propagateDefault, sender, value})
    end

    def map(aDNode, source, _sender, value) do
        #IO.puts "map #{inspect aDNode.id} #{inspect value}"
        res = aDNode.fct.(value)
        Enum.map(aDNode.children, &DNode.propagate(&1, source, aDNode.id, res))
        aDNode
    end

    def latest(aDNode, source, sender, value) do
        #IO.puts "latest #{inspect aDNode.id} #{inspect value}"
        aDNode = %{ aDNode | args: %{ aDNode.args | sender => value } }
        args = Enum.map(aDNode.parents, &Map.get(aDNode.args, &1))
        res = apply(aDNode.fct, args)
        Enum.map(aDNode.children, &DNode.propagate(&1, source, aDNode.id, res))
        aDNode
    end

    def checkDeps(aDNode, source, sender, value) do
        #IO.puts "checkDeps #{inspect aDNode.id} #{inspect value}"
        aDNode = update_in(aDNode.buffs[source][sender], &(:queue.in(value, &1)))
        ready = Enum.reduce_while(aDNode.buffs[source], true, fn {_, queue}, acc ->
            empty = :queue.is_empty(queue)
            { empty && :halt || :cont, acc && !empty }
        end)
        if ready do
            aDNode = Enum.reduce(aDNode.buffs[source], aDNode, fn {parentId, queue}, aDNode ->
                {{:value, value}, queue} = :queue.out(queue)
                %{ aDNode |
                    buffs: %{ aDNode.buffs | source => Map.put(aDNode.buffs[source], parentId, queue) },
                    args: Map.put(aDNode.args, parentId, value)
                }
            end)
            args = Enum.map(aDNode.parents, &Map.get(aDNode.args, &1))
            res = apply(aDNode.fct, args)
            Enum.map(aDNode.children, &DNode.propagate(&1, source, aDNode.id, res))
            aDNode
        else
            aDNode
        end
    end

    # Server API

    def init(%__MODULE__{}=aDNode) do
        { :ok, aDNode }
    end

    def handle_cast({:propagate, source, sender, value}, aDNode) do 
        { :noreply, aDNode.onReceive.(aDNode, source, sender, value) }
    end

    def handle_cast({:propagateDefault, sender, parentDefault}, aDNode) do 
        #IO.puts "propagateDefault #{inspect aDNode.id} #{inspect sender}"
        aDNode = %{ aDNode | args: %{ aDNode.args | sender => parentDefault } }
        if Enum.count(aDNode.args, fn {_, arg} -> arg==%Sentinel{} end)==0 do
            if length(aDNode.children)>0 do
                default = apply(aDNode.fct, Enum.map(aDNode.parents, &Map.get(aDNode.args, &1)))
                Enum.map(aDNode.children, &DNode.propagateDefault(&1, aDNode.id, default))
            else
                apply(aDNode.fct, Enum.map(aDNode.parents, &Map.get(aDNode.args, &1)))
                Enum.map(aDNode.parents, &send(&1, :start))
            end
        end
        { :noreply, aDNode}
    end

    def handle_info(:start, aDNode) do
        #IO.puts "start #{inspect aDNode.id} "
        aDNode = %{ aDNode | startReceived: aDNode.startReceived+1 }
        if length(aDNode.children)==aDNode.startReceived do
            Enum.map(aDNode.parents, &send(&1, :start))
        end
        { :noreply, aDNode }
    end 

end