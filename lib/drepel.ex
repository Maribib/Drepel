#require Drepel.Env
require Drepel.Supervisor
require MockDNode

defmodule Drepel do
    use Application

    def start(_type, _args) do
        Drepel.Supervisor.start_link()
    end

    def stop(_state \\ nil) do
        Drepel.Env.stopAllNodes()
    end

    def resetEnv do
        Drepel.Env.reset()
    end

    def miliseconds(amount, opts \\ []) do
        Drepel.Env.createSource(amount, fn state -> {state, state+1} end, 1, 0, opts)
    end

    def seconds(amount, opts \\ []) do
        Drepel.Env.createSource(amount*1000, fn state -> {state, state+1} end, 1, 0, opts)
    end
    
    def newSignal(parents, fct, opts \\ [])
    def newSignal(parents, fct, opts) when is_list(parents) and is_function(fct) do
        if length(parents)>0 do
            if :erlang.fun_info(fct)[:arity]==length(parents) do
                Drepel.Env.createNode(Enum.map(parents, fn %MockDNode{id: id} -> id end), fct, opts)
            else
                throw "The arity of the function must be equal to the number of parents."
            end
        else
            throw "Signal must have a least one parent."
        end
    end

    def newSignal(%MockDNode{}=parent, fct, opts) when is_function(fct) do
        newSignal([parent], fct, opts)
    end

    def map(%MockDNode{}=parent, fct, opts \\ []) when is_function(fct) do
        newSignal([parent], fct, opts)
    end

    def scan(%MockDNode{id: id}, initState, fct, opts \\ []) when is_function(fct) do
        if :erlang.fun_info(fct)[:arity]==2 do
            Drepel.Env.createStatedNode([id], fct, initState, opts)
        else
            throw "The arity of the function must be equal to 2."
        end
    end

    def reduce(%MockDNode{}=parent, initState, fct, opts \\ []) when is_function(fct) do
        scan(parent, initState, fct, opts)
    end

    def filter(%MockDNode{}=parent, initState, condition, opts \\ []) when is_function(condition) do
        if :erlang.fun_info(condition)[:arity]==1 do
            fct = fn new, old -> 
                condition.(new) && new || old
            end
            reduce(parent, initState, fct, opts)
        else
            throw "The arity of the condition function must be 1."
        end
    end

    def run(duration \\ :inf) do
        Drepel.Env.startAllNodes()
        Process.monitor(Drepel.Supervisor)
        res = case duration do
            :inf -> receive do
                _msg -> :done
            end
            _ -> receive do
                _msg -> :done
            after
                duration -> :stopped
            end
        end
        Drepel.Env.stopAllNodes()
        res
    end

end

