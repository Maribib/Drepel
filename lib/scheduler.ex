defmodule Scheduler do
    use Task

    def start_link(_args, _opts \\ nil) do
        res = Task.start_link(&Scheduler.run/0)
        Process.register(elem(res, 1), __MODULE__)
        res
    end

    def schedule(nod, now, time, name \\ nil, onRun \\ nil, eid \\ nil)
    def schedule(%DNode{id: id, reorderer: reorderer}, now, time, name, onRun, eid) when is_nil(reorderer) do # online scheduling to reorderer
        send(__MODULE__, Schedule.shiftMilisec(now, %Event{id: id, time: time, name: name, onRun: onRun, eid: eid}))
    end
    def schedule(%DNode{id: id, reorderer: reorderer}, now, time, name, onRun, eid) do # online scheduling
        send(__MODULE__, Schedule.shiftMilisec(now, %Event{id: reorderer, time: time, name: {:val, id, name}, onRun: onRun, eid: eid}))
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