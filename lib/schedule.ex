require RedBlackTree.Utils

defmodule Schedule do
    use GenServer

    def _comparator(e1, e2) do
        case Timex.compare(e1.time, e2.time) do
            0 -> case RedBlackTree.compare_terms(e1.id, e2.id) do
                0 -> RedBlackTree.compare_terms(e1.eid, e2.eid)
                res -> res
            end
            res -> res
        end
    end

    def shiftMilisec(now, event) do
        %{ event | time: Timex.shift(now, microseconds: event.time*1000) }
    end

    # Client API

    def start_link(_args, _opts \\ nil) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def getSleepTime() do
        GenServer.call(__MODULE__, {:getSleepTime})
    end

    def spawnAll() do
        GenServer.call(__MODULE__, {:spawnAll})
    end

    def addNewEvent(event) do
        GenServer.cast(__MODULE__, {:addNewEvent, event})
    end

    # Server API

    def handle_call({:getSleepTime}, _from, schedule) do
        if (Set.size(schedule)>0) do
            { :reply, max(div(Timex.diff(RedBlackTree.Utils.first(schedule).time, Timex.now), 1000), 0), schedule }
        else
            { :reply, 1_000, schedule }
        end
    end

    def handle_call({:spawnAll}, _from, schedule) do
        now = Timex.now
        { :reply, :ok, _spawnAll(now, schedule) }
    end

    def handle_cast({:addNewEvent, event}, schedule) do
        { :noreply, RedBlackTree.insert(schedule, event) }
    end

    def _spawnAll(now, schedule) do
        case RedBlackTree.Utils.first(schedule) do
            nil -> schedule
            event -> _spawnOne(schedule, event, now)
        end 
    end

    def _spawnOne(schedule, event, now) do
        if Timex.after?(now, event.time) do # check if time to spawn
            DNode.onScheduled(event.id, event.name, event.time)
            schedule = RedBlackTree.delete(schedule, event)
            if !is_nil(event.onRun) do # check if rescheduling rule
                newEvent = event.onRun.(event)
                _spawnAll(now, RedBlackTree.insert(schedule, newEvent) )
            else
                _spawnAll(now, schedule)
            end
        else
            schedule
        end
    end

    def init(:ok) do
        events = Enum.map(EventCollector.getEvents(), &shiftMilisec(Timex.now, &1))
        GenServer.stop(EventCollector)
        {:ok, RedBlackTree.new(events, comparator: &Schedule._comparator/2) }
    end

end