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

    defmodule Source do
        def miliseconds(amount, opts \\ []) do
            Drepel.Env.createSource(amount, fn state -> {state, state+1} end, 1, 0, opts)
        end

        def seconds(amount, opts \\ []) do
            Drepel.Env.createSource(amount*1000, fn state -> {state, state+1} end, 1, 0, opts)
        end
    end

    defmodule Signal do
        def new(parents, fct, opts \\ [])
        def new(parents, fct, opts) when is_list(parents) and is_function(fct) do
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

        def new(%MockDNode{}=parent, fct, opts) when is_function(fct) do
            new([parent], fct, opts)
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

