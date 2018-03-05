
defmodule Signal do
    @enforce_keys [:id, :fct, :args]
    defstruct [ :id, :fct, :args, :dependencies, :buffs, :default,
    :chckpts, :leader,
    parents: [], children: [], startReceived: 0, state: %Sentinel{}, 
    hasChildren: false, chckptId: 0, repNodes: [], routing: %{} ]

    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, aSignal) do
        GenServer.start_link(__MODULE__, aSignal, name: aSignal.id)
    end

    def getStats(id) do
        GenServer.call(id, :getStats)
    end

    def propagate(aNode, id, message) do
        GenServer.cast({id, aNode}, {:propagate, message})
    end

    def _propagate(%__MODULE__{}=aSignal, message, value) do
        if aSignal.hasChildren do
            message = %{ message |
                sender: aSignal.id,
                value: value
            }
            Enum.map(aSignal.children, &__MODULE__.propagate(Map.get(aSignal.routing, &1), &1, message))
        else
            delta = :os.system_time(:microsecond) - message.timestamp
            Drepel.Stats.updateLatency(delta)
        end
    end

    def propagateDefault(aNode, id, sender, value) do
        GenServer.cast({id, aNode}, {:propagateDefault, sender, value})
    end

    def propagateChckpt(aNode, id, message) do
        GenServer.cast({id, aNode}, {:propagateChckpt, message})
    end

    def _apply(aSignal, args) do
        {duration, res} = :timer.tc(aSignal.fct, args)
        Drepel.Stats.updateWork(aSignal.id, duration)
        res
    end

    def purge(aSignal) do
        Enum.reduce(aSignal.buffs, aSignal, fn {source, sourceBuffs}, aSignal -> 
            _purge(aSignal, source, sourceBuffs)
        end)
    end

    def _purge(aSignal, source, sourceBuffs) do
        ready = Enum.reduce_while(sourceBuffs, true, fn {_, queue}, acc ->
            ready = !:queue.is_empty(queue) && :queue.head(queue).chckptId==aSignal.chckptId
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            aSignal = consume(aSignal, source)
            _purge(aSignal, source, sourceBuffs)
        else
            aSignal
        end
    end

    # Server API

    def init(%__MODULE__{dependencies: deps}=aSignal) do
        {:ok, %{ aSignal | 
            hasChildren: length(aSignal.children)>0, 
            chckpts: Enum.reduce(aSignal.parents, %{}, &Map.put(&2, &1, 0)),
            buffs: Enum.reduce(deps, %{}, fn {source, parents}, acc ->
                sourceBuffs = Enum.reduce(parents, %{}, fn parentId, acc ->
                    Map.put(acc, parentId, :queue.new()) 
                end)
                Map.put(acc, source, sourceBuffs)
            end)
        } }
    end

    def handle_call(:getStats, _from, aSignal) do
        { :reply, Map.take(aSignal, [:cnt, :sum]), aSignal }
    end

    def consume(aSignal, source) do
        {aSignal, message} = Enum.reduce(aSignal.buffs[source], {aSignal, nil}, fn {parentId, queue}, {aSignal, _} ->
            {{:value, message}, queue} = :queue.out(queue)
            {
                %{ aSignal |
                    buffs: %{ aSignal.buffs | source => Map.put(aSignal.buffs[source], parentId, queue) },
                    args: Map.put(aSignal.args, parentId, message.value)
                },
                message
            }
        end)
        args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
        case aSignal.state do
            %Sentinel{} -> 
                res = _apply(aSignal, args)
                _propagate(aSignal, message, res)
                aSignal
            _ -> 
                {res, state} = _apply(aSignal, args ++ [aSignal.state])
                _propagate(aSignal, message, res)
                %{ aSignal | state: state }
        end 
    end

    def handle_cast({:propagate, %Message{sender: sender, source: source}=message}, aSignal) do 
        #IO.puts "qprops #{inspect aSignal.id} #{inspect value}"
        aSignal = update_in(aSignal.buffs[source][sender], &(:queue.in(message, &1)))
        ready = Enum.reduce_while(aSignal.buffs[source], true, fn {_, queue}, acc ->
            chckpting = Map.get(aSignal.chckpts, sender)>0
            empty = :queue.is_empty(queue)
            ready = !(empty || chckpting)
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            { :noreply, consume(aSignal, source) }
        else
            { :noreply, aSignal }
        end
    end

    def handle_cast({:propagateChckpt, message}, aSignal) do
        aSignal = update_in(aSignal.chckpts[message.sender], &(&1 + 1))
        ready = Enum.reduce_while(aSignal.chckpts, true, fn {_, cnt}, acc ->
            ready = cnt>0
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            Enum.map(aSignal.repNodes, &Store.put(&1, message.id, aSignal))
            if !aSignal.hasChildren do
                Checkpoint.completed(aSignal.leader, aSignal.id, message.id)
            end
            chckpts = Enum.reduce(aSignal.chckpts, %{}, fn {k, v}, acc -> Map.put(acc, k, v-1) end)
            Enum.map(aSignal.children, &__MODULE__.propagateChckpt(Map.get(aSignal.routing, &1), &1, %{ message | sender: aSignal.id}))
            aSignal = purge(%{ aSignal | chckpts: chckpts, chckptId: message.id })
            { :noreply, aSignal }
        else
            { :noreply, aSignal }
        end
    end

    def handle_cast({:propagateDefault, sender, parentDefault}, aSignal) do 
        #IO.puts "propagateDefault #{inspect aSignal.id} #{inspect sender}"
        aSignal = %{ aSignal | args: %{ aSignal.args | sender => parentDefault } }
        aSignal = if Enum.count(aSignal.args, fn {_, arg} -> arg==%Sentinel{} end)==0 do
            if length(aSignal.children)>0 do
                args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
                { default, state } = case aSignal.state do
                    %Sentinel{} -> 
                        { apply(aSignal.fct, args), aSignal.state }
                    _ -> 
                        apply(aSignal.fct, args ++ [aSignal.state])
                end
                Enum.map(aSignal.children, &__MODULE__.propagateDefault(Map.get(aSignal.routing, &1), &1, aSignal.id, default))
                %{ aSignal | state: state }
            else
                apply(aSignal.fct, Enum.map(aSignal.parents, &Map.get(aSignal.args, &1)))
                Enum.map(aSignal.parents, &send({&1, Map.get(aSignal.routing, &1)}, :start))
                aSignal
            end
        else
            aSignal
        end
        { :noreply, aSignal }
    end

    def handle_info(:start, aSignal) do
        #IO.puts "start #{inspect aSignal.id} "
        aSignal = %{ aSignal | startReceived: aSignal.startReceived+1 }
        if length(aSignal.children)==aSignal.startReceived do
            Enum.map(aSignal.parents, &send({&1, Map.get(aSignal.routing, &1)}, :start))
        end
        { :noreply, aSignal }
    end 

end