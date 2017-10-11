require Scheduler
require MockDNode
require Drepel.Env

defmodule Drepel.Supervisor do
    def new() do
        Agent.start_link(fn -> %{schedule: Scheduler.initSchedule(), ps: []} end, name: __MODULE__)
        pid = anonWorker(&Scheduler.run/0)
        Process.register(pid, :scheduler)
        env = Drepel.Env.get()
        anonWorker(fn -> Enum.map(env.children, &Map.get(env.nodes, &1).runFct.(%MockDNode{id: &1})) end)
    end

    def anonWorker(fct) do
        Agent.get_and_update(__MODULE__, fn s -> 
            pid = elem(Task.start_link(fct), 1)
            {pid, %{ s | ps: s.ps ++ [pid]} }
        end)
    end

    def closeAll do
        if Agent.get_and_update(__MODULE__, fn s -> 
            news = %{ s | ps: 
                Enum.filter(s.ps, fn p ->
                    if Process.alive?(p) do
                        Process.exit(p, :normal)
                    end
                    Process.alive?(p)
                end)
            }
            {length(news.ps)>0, news}
        end) do
            closeAll()
        end
    end

    def join(duration) do
        if duration==:inf do
           _join()
        else
            Task.async(&_join/0)
            |> Task.yield(duration)
        end
        closeAll()
    end

    def _join do
        {runningPs, scheduledPs} = Agent.get_and_update(__MODULE__, &checkExecState/1)
        if (runningPs>1 || scheduledPs>0) do
            _join()
        end
    end

    def checkExecState(%{ps: ps, schedule: schedule} = state) do
        runningPs = Enum.filter(ps, &Process.alive?(&1))
        {{length(runningPs), Set.size(schedule)}, %{state | ps: runningPs} }
    end

end