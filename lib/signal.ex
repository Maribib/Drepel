defmodule Signal do
    @enforce_keys [:id, :fct, :args]
    defstruct [ :id, :fct, :args, :dependencies, :buffs, :default,
    :chckpts, :leader, :readyCnt,
    parents: [], children: [], startReceived: 0, state: %Sentinel{}, 
    hasChildren: false, routing: %{} ]

    use GenServer, restart: :transient
    
    def unblock(aSignal, chckptId) do
        Enum.reduce(aSignal.buffs, aSignal, fn {source, _}, aSignal -> 
            _unblock(aSignal, source, chckptId)
        end)
    end

    def _unblock(aSignal, source, chckptId) do
        if aSignal.readyCnt==Map.size(aSignal.buffs[source]) 
            && :queue.head(Enum.at(aSignal.buffs[source], 0) |> elem(1)).chckptId==chckptId do
            aSignal = reevaluate(aSignal, source)
            _unblock(aSignal, source, chckptId)
        else
            aSignal
        end
    end

    def _propagate(%__MODULE__{}=aSignal, msg, value) do
        msg = %{ msg |
            sender: aSignal.id,
            value: value
        }
        Enum.map(aSignal.children, &__MODULE__.propagate(Map.get(aSignal.routing, &1), &1, msg))
    end

    # Client API

    def start_link(aSignal) do
        GenServer.start_link(__MODULE__, aSignal, name: aSignal.id)
    end

    def propagate(aNode, id, msg) do
        GenServer.cast({id, aNode}, {:propagate, msg})
    end

    def propagateDefault(aNode, id, sender, value) do
        GenServer.cast({id, aNode}, {:propagateDefault, sender, value})
    end

    # Server API

    def init(%__MODULE__{dependencies: deps}=aSignal) do
        buffs = Enum.reduce(deps, %{}, fn {source, parents}, acc ->
            sourceBuffs = Enum.reduce(parents, %{}, fn parentId, acc ->
                Map.put(acc, parentId, :queue.new()) 
            end)
            Map.put(acc, source, sourceBuffs)
        end)
        {:ok, %{ aSignal | 
            hasChildren: length(aSignal.children)>0, 
            chckpts: Enum.reduce(aSignal.parents, %{}, &Map.put(&2, &1, 0)),
            buffs: buffs,
            readyCnt: Enum.reduce(buffs, %{}, fn {source, _}, acc ->
                Map.put(acc, source, 0)
            end)
        } }
    end

    def reevaluate(aSignal, source) do
        {aSignal, message} = Enum.reduce(aSignal.buffs[source], {aSignal, nil}, fn {parentId, queue}, {aSignal, _} ->
            {{:value, message}, queue} = :queue.out(queue)
            {
                %{ aSignal |
                    buffs: %{ aSignal.buffs | source => Map.put(aSignal.buffs[source], parentId, queue) },
                    args: Map.put(aSignal.args, parentId, message.value),
                    readyCnt: Map.update!(aSignal.readyCnt, source, fn val ->
                        if :queue.is_empty(queue) do
                            val-1
                        else
                            val
                        end
                    end)
                },
                message
            }
        end)
        args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
        case aSignal.state do
            %Sentinel{} -> 
                res = apply(aSignal.fct, args)
                _propagate(aSignal, message, res)
                aSignal
            _ -> 
                {res, state} = apply(aSignal.fct, args ++ [aSignal.state])
                _propagate(aSignal, message, res)
                %{ aSignal | state: state }
        end 
    end

    def handle_cast({:propagate, %Message{sender: sender, source: source}=msg}, aSignal) do 
        wasEmpty = :queue.is_empty(aSignal.buffs[source][sender])
        aSignal = update_in(aSignal.buffs[source][sender], &(:queue.in(msg, &1)))
        if wasEmpty do
            aSignal = update_in(aSignal.readyCnt[source], &(&1+1))
            if (Map.size(aSignal.buffs[source])==aSignal.readyCnt[source]) && Map.get(aSignal.chckpts, sender)==0 do
                { :noreply, reevaluate(aSignal, source) }
            else
                { :noreply, aSignal }
            end
        else
            { :noreply, aSignal }
        end
    end

    def handle_cast({:propagate, %ChckptMessage{id: id, sender: sender}=msg}, aSignal) do
        # track checkpoint
        aSignal = update_in(aSignal.chckpts[sender], &(&1 + 1))
        # ready if checkpoint message from all parents are received
        ready = Enum.reduce_while(aSignal.chckpts, true, fn {_, cnt}, acc ->
            ready = cnt>0
            { ready && :cont || :halt, acc && ready }
        end)
        if ready do
            Store.put(id, %{ aSignal | buffs: nil })
            if !aSignal.hasChildren do
                Checkpoint.completed(aSignal.leader, aSignal.id, id)
            end
            chckpts = Enum.reduce(aSignal.chckpts, %{}, fn {k, v}, acc -> Map.put(acc, k, v-1) end)
            Enum.map(aSignal.children, fn id ->
                node = Map.get(aSignal.routing, id)
                propagate(node, id, %{ msg | sender: aSignal.id})
            end)
            aSignal = unblock(%{ aSignal | chckpts: chckpts }, id)
            { :noreply, aSignal }
        else
            { :noreply, aSignal }
        end
    end

    def handle_cast({:propagateDefault, sender, parentDefault}, aSignal) do 
        #IO.puts "propagateDefault #{inspect aSignal.id} #{inspect sender}"
        aSignal = %{ aSignal | args: %{ aSignal.args | sender => parentDefault } }
        aSignal = if Enum.count(aSignal.args, fn {_, arg} -> arg==%Sentinel{} end)==0 do
            args = Enum.map(aSignal.parents, &Map.get(aSignal.args, &1))
            { default, state } = case aSignal.state do
                %Sentinel{} -> 
                    { apply(aSignal.fct, args), aSignal.state }
                _ -> 
                    apply(aSignal.fct, args ++ [aSignal.state])
            end
            if length(aSignal.children)>0 do 
                Enum.map(aSignal.children, fn id ->
                    node = Map.get(aSignal.routing, id)
                    propagateDefault(node, id, aSignal.id, default)
                end)
            else
                Enum.map(aSignal.parents, fn id ->
                    node = Map.get(aSignal.routing, id)
                    send({id, node}, :start)
                end)
            end
            # copy initial state in case of failure before first checkpoint completion
            Store.put(-1, aSignal )
            %{ aSignal | state: state }
        else
            aSignal
        end
        { :noreply, aSignal }
    end

    def handle_info(:start, aSignal) do
        aSignal = %{ aSignal | startReceived: aSignal.startReceived+1 }
        # check if all "start" messages are received
        if length(aSignal.children)==aSignal.startReceived do
            Enum.map(aSignal.parents, &send({&1, Map.get(aSignal.routing, &1)}, :start))
        end
        { :noreply, aSignal }
    end

end