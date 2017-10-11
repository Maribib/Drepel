require RedBlackTree.Utils

defmodule Event do
    @enforce_keys [ :id, :time, :toRun, :onRun ]
    defstruct [ :id, :time, :toRun, :onRun ]
end

defmodule Scheduler do
    def new do
        Agent.start_link(fn -> [] end, name: :eventCollection)
    end

    def schedule(map, time, toRun, onRun \\ nil)
    def schedule(%MockDNode{id: id}, time, toRun, onRun) do # online scheduling
        send(:scheduler, %Event{id: id, time: time, toRun: toRun, onRun: onRun})
    end
    def schedule(%DNode{id: id}, time, toRun, onRun) do # offline scheduling
        Agent.update(:eventCollection, &(&1 ++ [ %Event{id: id, time: time, toRun: toRun, onRun: onRun} ]))
    end

    def _comparator(e1, e2) do
        case Timex.compare(e1.time, e2.time) do
            0 -> RedBlackTree.compare_terms(e1.id, e2.id)
            res -> res
        end
    end

    def spawnAll do
        now = Timex.now
        Agent.update(Drepel.Env, &_spawnAll(now, &1))
    end

    def _spawnAll(now, %{ schedule: schedule } = env) do
        case RedBlackTree.Utils.first(schedule) do
            nil -> env
            v -> _spawnOne(env, v, now)
        end 
    end

    def _spawnOne(%{ workers: workers, schedule: schedule } = env, event, now) do
        if Timex.after?(now, event.time) do # check if time to spawn
            pid = elem(Task.start_link(fn -> event.toRun.(%MockDNode{id: event.id}) end), 1)
            schedule = RedBlackTree.delete(schedule, event)
            if !is_nil(event.onRun) do # check if rescheduling rule
                newEvent = event.onRun.(event)
                _spawnAll(now, %{ env | schedule: RedBlackTree.insert(schedule, newEvent), workers: workers ++ [pid] })
            else
                _spawnAll(now, %{ env | schedule: schedule, workers: workers ++ [pid] })
            end
        else
            env
        end
    end

    def sleepTime do
        Agent.get(Drepel.Env, fn %{schedule: schedule} ->
            if (Set.size(schedule)>0) do
                max(div(Timex.diff(RedBlackTree.Utils.first(schedule).time, Timex.now), 1000), 0)
            else
                1_000
            end
        end)
    end

    def shiftMilisec(now, event) do
        %{ event | time: Timex.shift(now, microseconds: event.time*1000) }
    end

    def initSchedule do
        now = Timex.now
        events = Enum.map(Agent.get(:eventCollection, &(&1)), &shiftMilisec(now, &1))
        Agent.stop(:eventCollection)
        RedBlackTree.new(events, comparator: &Scheduler._comparator/2)
    end

    def run do
        Process.flag(:trap_exit, true)
        Agent.update(Drepel.Env, fn env -> %{ env | schedule: Scheduler.initSchedule() } end)
        _run()
    end

    def _run do
        spawnAll()
        case sleepTime() do
            0 -> Scheduler._run
            t -> receive do
                {:EXIT, pid, :normal} -> nil
            after 
                t -> Scheduler._run
            end
        end
    end
   
end