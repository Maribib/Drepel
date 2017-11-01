defmodule Scheduler do
    use Task

    def start_link(_args, _opts \\ nil) do
        res = Task.start_link(&Scheduler.run/0)
        Process.register(elem(res, 1), __MODULE__)
        res
    end

    def schedule(%DNode{id: id}, time, name \\ nil, onRun \\ nil, eid \\ nil) do # online scheduling
        send(__MODULE__, Schedule.shiftMilisec(Timex.now, %Event{id: id, time: time, name: name, onRun: onRun, eid: eid}))
    end

    def run do
        Schedule.spawnAll()
        case Schedule.getSleepTime() do
            0 -> Scheduler.run
            t -> receive do
                %Event{}=ev -> 
                    Schedule.addNewEvent(ev)
                    Scheduler.run
            after 
                t -> Scheduler.run
            end
        end
    end
   
end