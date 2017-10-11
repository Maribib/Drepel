require Scheduler
require MockDNode
require Drepel.Env

defmodule Drepel.Supervisor do
    def new() do
        pid = anonWorker(&Scheduler.run/0)
        Process.register(pid, :scheduler)
        env = Drepel.Env.get()
        Enum.map(env.children, fn id -> anonWorker(fn -> Map.get(env.nodes, id).runFct.(%MockDNode{id: id}) end) end)
    end

    def anonWorker(fct) do
        Agent.get_and_update(Drepel.Env, fn env -> 
            pid = elem(Task.start_link(fct), 1)
            {pid, %{ env | workers: env.workers ++ [pid]} }
        end)
    end

    def closeAll do
        if Agent.get_and_update(Drepel.Env, fn env -> 
            news = %{ env | workers: 
                Enum.filter(env.workers, fn pid ->
                    if Process.alive?(pid) do
                        Process.exit(pid, :normal)
                    end
                    Process.alive?(pid)
                end)
            }
            { length(news.workers)>0, news }
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
        {runningPs, scheduledPs} = Agent.get_and_update(Drepel.Env, &checkExecState/1)
        if (runningPs>1 || scheduledPs>0) do
            _join()
        end
    end

    def checkExecState(%{workers: workers, schedule: schedule} = state) do
        runningPs = Enum.filter(workers, &Process.alive?(&1))
        { {length(runningPs), Set.size(schedule)}, %{state | workers: runningPs} }
    end

end