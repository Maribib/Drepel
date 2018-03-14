require Drepel.Supervisor
require MockNode

defmodule Drepel do
    use Application

    def start(_type, _args) do
        Drepel.Supervisor.start_link()
    end

    def stop(_state \\ nil) do
        Drepel.Env.stopNodes()
    end

    def test() do
        s1 = Drepel.milliseconds(3) 
        s2 = Drepel.milliseconds(2, node: :"bar@MB")
        s3 = Drepel.milliseconds(2, node: :"bar@MB")
        x1 = Drepel.signal([s1, s2], fn x,y -> x+y end, node: :"bar@MB")
        x2 = Drepel.signal([s2, s3], fn y,z -> y*y+z*z end, node: :"bar@MB")
        Drepel.run()
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

    def source(rate, default, fct, opts \\ []) when is_integer(rate) do
        Drepel.Env.createSource(rate, fct, default, opts)
    end

    def milliseconds(rate \\ 100, opts \\ []) when is_integer(rate) do
        fct = fn -> :os.system_time(:millisecond) end
        __MODULE__.source(rate, fct, fct, opts)
    end

    def seconds(rate \\ 1000, opts \\ []) when is_integer(rate) do
        fct = fn -> :os.system_time(:second) end
        __MODULE__.source(rate, fct, fct, opts)
    end

    def eventSource(port, default, opts \\ []) when is_integer(port) do
        Drepel.Env.createEventSource(port, default, opts)
    end
    
    def signal(parents, fct, opts \\ [])
    def signal(parents, fct, opts) when is_list(parents) and is_function(fct) do
        if length(parents)>0 do
            if :erlang.fun_info(fct)[:arity]==length(parents) do
                Drepel.Env.createSignal(Enum.map(parents, fn %MockNode{id: id} -> id end), fct, opts)
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

    def map(%MockNode{}=parent, fct, opts \\ []) when is_function(fct) do
        signal([parent], fct, opts)
    end

    def scan(%MockNode{id: id}, initState, fct, opts \\ []) when is_function(fct) do
        if :erlang.fun_info(fct)[:arity]==2 do
            Drepel.Env.createStatedNode([id], fct, initState, opts)
        else
            throw "The arity of the function must be equal to 2."
        end
    end

    def reduce(%MockNode{}=parent, initState, fct, opts \\ []) when is_function(fct) do
        scan(parent, initState, fct, opts)
    end

    def filter(%MockNode{}=parent, initState, predicate, opts \\ []) when is_function(predicate) do
        if :erlang.fun_info(predicate)[:arity]==1 do
            fct = fn new, old -> 
                res = predicate.(new) && new || old 
                { res, res }
            end
            reduce(parent, initState, fct, opts)
        else
            throw "The arity of the predicate function must be 1."
        end
    end

    def run(duration \\ :inf) do
        Drepel.Env.startNodes()
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
        Drepel.Env.stopNodes()
        res
    end

end

