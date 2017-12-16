
defmodule Signal do
    @enforce_keys [:id, :fct, :onReceive, :args, :buffs]
    defstruct [ :id, :fct, :onReceive, :args, :dependencies, :buffs,
    parents: [], children: [], startReceived: 0, state: %Sentinel{}, ]

    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, aSignal) do
        {id, _node} = aSignal.id
        GenServer.start_link(__MODULE__, aSignal, name: id)
    end

    def propagate(id, source, sender, value) do
        GenServer.cast(id, {:propagate, source, sender, value})
    end

    def propagateDefault(id, sender, value) do
        GenServer.cast(id, {:propagateDefault, sender, value})
    end

    def map(aSignal, source, _sender, value) do
        #IO.puts "map #{inspect aSignal.id} #{inspect value}"
        res = aSignal.fct.(value)
        Enum.map(aSignal.children, &__MODULE__.propagate(&1, source, aSignal.id, res))
        aSignal
    end

    def scan(aSignal, source, _sender, value) do
        res = aSignal.fct.(value, aSignal.state)
        Enum.map(aSignal.children, &__MODULE__.propagate(&1, source, aSignal.id, res))
        %{ aSignal | state: res }
    end

    def latest(aSignal, source, sender, value) do
        #IO.puts "latest #{inspect aSignal.id} #{inspect value}"
        aSignal = %{ aSignal | args: %{ aSignal.args | sender => value } }
        args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
        res = apply(aSignal.fct, args)
        Enum.map(aSignal.children, &__MODULE__.propagate(&1, source, aSignal.id, res))
        aSignal
    end

    def checkDeps(aSignal, source, sender, value) do
        #IO.puts "checkDeps #{inspect aSignal.id} #{inspect value}"
        aSignal = update_in(aSignal.buffs[source][sender], &(:queue.in(value, &1)))
        ready = Enum.reduce_while(aSignal.buffs[source], true, fn {_, queue}, acc ->
            empty = :queue.is_empty(queue)
            { empty && :halt || :cont, acc && !empty }
        end)
        if ready do
            aSignal = Enum.reduce(aSignal.buffs[source], aSignal, fn {parentId, queue}, aSignal ->
                {{:value, value}, queue} = :queue.out(queue)
                %{ aSignal |
                    buffs: %{ aSignal.buffs | source => Map.put(aSignal.buffs[source], parentId, queue) },
                    args: Map.put(aSignal.args, parentId, value)
                }
            end)
            args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
            res = apply(aSignal.fct, args)
            Enum.map(aSignal.children, &__MODULE__.propagate(&1, source, aSignal.id, res))
            aSignal
        else
            aSignal
        end
    end

    # Server API

    def init(%__MODULE__{}=aSignal) do
        {:ok, aSignal }
    end

    def handle_cast({:propagate, source, sender, value}, aSignal) do 
        { :noreply, aSignal.onReceive.(aSignal, source, sender, value) }
    end

    def handle_cast({:propagateDefault, sender, parentDefault}, aSignal) do 
        #IO.puts "propagateDefault #{inspect aSignal.id} #{inspect sender}"
        aSignal = %{ aSignal | args: %{ aSignal.args | sender => parentDefault } }
        if Enum.count(aSignal.args, fn {_, arg} -> arg==%Sentinel{} end)==0 do
            if length(aSignal.children)>0 do
                default = case aSignal.state do
                    %Sentinel{} -> apply(aSignal.fct, Enum.map(aSignal.parents, &Map.get(aSignal.args, &1)))
                    _ -> aSignal.state
                end
                Enum.map(aSignal.children, &__MODULE__.propagateDefault(&1, aSignal.id, default))
            else
                apply(aSignal.fct, Enum.map(aSignal.parents, &Map.get(aSignal.args, &1)))
                Enum.map(aSignal.parents, &send(&1, :start))
            end
        end
        { :noreply, aSignal}
    end

    def handle_info(:start, aSignal) do
        #IO.puts "start #{inspect aSignal.id} "
        aSignal = %{ aSignal | startReceived: aSignal.startReceived+1 }
        if length(aSignal.children)==aSignal.startReceived do
            Enum.map(aSignal.parents, &send(&1, :start))
        end
        { :noreply, aSignal }
    end 

end