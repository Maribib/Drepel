defmodule Scheduler do
    use Task

    def start_link(args, _opts \\ nil) do
        Task.start_link(&Scheduler.run/0)
    end

    def schedule(%DNode{id: id}, time, toRun, onRun) do # online scheduling
        send(:schedule, %Event{id: id, time: time, toRun: toRun, onRun: onRun})
    end

    def run do
        Schedule.spawnAll()
        case Schedule.getSleepTime() do
            0 -> Scheduler.run
            t -> receive do
                {:schedule, %Event{}=ev} -> nil # todo insert in schedule
            after 
                t -> Scheduler.run
            end
        end
    end
   
end