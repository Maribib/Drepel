require MockNode

defmodule Drepel do
    use Application

    def start(_type, _args) do
        Main.Supervisor.start_link()
    end

    def stop(_state \\ nil) do
        Drepel.Env.stopNodes()
    end

    def resetEnv do
        Drepel.Env.reset()
    end

    def setCheckpointInterval(interval) when is_integer(interval) do
        if interval>=0 do
            Drepel.Env.setCheckpointInterval(interval)
        else
            throw "The interval value must be positive to enable checkpointing.
            Use 0 value to disable checkpointing."
        end
    end

    def setBalancingInterval(interval) when is_integer(interval) do
        Drepel.Env.setBalancingInterval(interval)
    end

    def setReplicationFactor(factor) when is_integer(factor) do
        if factor>=0 do
            Drepel.Env.setReplicationFactor(factor)
        else
            throw "The replication factor must be a positive or null integer."
        end
    end

    def setClusterNodes(nodes) when is_list(nodes) do
        Drepel.Env.setClusterNodes(nodes)
    end

    def bSource(rate, default, fct, opts \\ []) when is_integer(rate) do
        Drepel.Env.createBSource(rate, fct, default, opts)
    end

    def milliseconds(rate \\ 100, opts \\ []) when is_integer(rate) do
        fct = fn -> :os.system_time(:millisecond) end
        Drepel.bSource(rate, fct, fct, opts)
    end

    def seconds(rate \\ 1000, opts \\ []) when is_integer(rate) do
        fct = fn -> :os.system_time(:second) end
        Drepel.bSource(rate, fct, fct, opts)
    end

    def eSource(name, default, opts \\ []) when is_binary(name) do
        Drepel.Env.createESource(name, default, opts)
    end
    
    def signal(parents, fct, opts \\ [])
    def signal(parents, fct, opts) when is_list(parents) and is_function(fct) do
        if length(parents)>0 do
            if :erlang.fun_info(fct)[:arity]==length(parents) do
                Enum.map(parents, fn %MockNode{id: id} -> id end)
                |> Drepel.Env.createSignal(fct, opts)
            else
                throw "The arity of the function must be equal to the number of parents."
            end
        else
            throw "Signal must have a least one parent."
        end
    end

    def signal(%MockNode{}=parent, fct, opts) when is_function(fct) do
        signal([parent], fct, opts)
    end

    def stateSignal(parents, initState, fct, opts \\ [])
    def stateSignal(parents, initState, fct, opts) when is_function(fct) and is_list(parents) do 
        if :erlang.fun_info(fct)[:arity]==length(parents)+1 do
            Enum.map(parents, fn %MockNode{id: id} -> id end)
            |> Drepel.Env.createStatedNode(fct, initState, opts)
        else 
            throw "The arity of the function must be equal to #parents + 1."
        end
    end

    def stateSignal(%MockNode{}=parent, initState, fct, opts) when is_function(fct) do 
        stateSignal([parent], initState, fct, opts)
    end

    def filter(%MockNode{}=parent, initState, predicate, opts \\ []) when is_function(predicate) do
        if :erlang.fun_info(predicate)[:arity]==1 do
            fct = fn new, old -> 
                res = predicate.(new) && new || old 
                { res, res }
            end
            stateSignal(parent, initState, fct, opts)
        else
            throw "The arity of the predicate function must be 1."
        end
    end

    def run(duration \\ :inf) do
        Drepel.Env.startNodes(duration)
    end

    def join(nodes) when is_list(nodes) do
        Drepel.Env.join(nodes)
    end 

    def join(node) do
        join([node])
    end

end

