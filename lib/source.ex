require Signal

defmodule Source do 
	@enforce_keys [ :id, :refreshRate, :fct, :default ]
	defstruct [ :id, :refreshRate, :fct, :default, :dependencies, :chckptInterval, 
    :clustNodes, :repNodes, :chckptInterval,
	children: [], startReceived: 0, prodTimer: nil, repNodes: [], chckptTimer: nil,
    chckptId: 0, routing: %{}]

	use GenServer, restart: :transient

	# Client API

	def start_link(_opts, aSource) do
		GenServer.start_link(__MODULE__, aSource, name: aSource.id)
    end

    def start_link(_opts, aSource, messages) do
        GenServer.start_link(__MODULE__, {aSource, messages}, name: aSource.id)
    end

	# Server API

	def init(%__MODULE__{}=aSource) do
		Process.flag(:trap_exit, true)
		defaultValue = case aSource.default do
			d when is_function(d) -> d.()
			_ -> aSource.default
		end
		Enum.map(aSource.children, &Signal.propagateDefault(Map.get(aSource.routing, &1), &1, aSource.id, defaultValue))
        { :ok, aSource }
    end

    def init({%__MODULE__{}=aSource, messages}) do
        IO.puts "restarted"
        Process.flag(:trap_exit, true)
        chckptTimer = if aSource.chckptInterval>0 do
            Process.send_after(aSource.id, :checkpoint, aSource.chckptInterval)
        else
            nil
        end 
        chckptId = Enum.reduce(messages, aSource.chckptId, fn message, acc ->
            {propFct, chckptId} = case message do
                %ChckptMessage{id: id} -> {&Signal.propagateChckpt/3, id+1}
                %Message{} -> {&Signal.propagate/3, acc}
            end
            Enum.map(aSource.children, &propFct.(Map.get(aSource.routing, &1), &1, message))
            chckptId
        end)
        {:ok, %{ aSource |
            prodTimer: Process.send_after(aSource.id, :produce, aSource.refreshRate),
            chckptTimer: chckptTimer,
            chckptId: chckptId
        } }
    end

    def terminate(_reason, aSource) do
        timers = [:prodTimer, :chckptTimer]
        Enum.map(timers, &Utils.cancelTimer(Map.get(aSource, &1)))
    end

    def handle_info(:produce, aSource) do
    	#IO.puts "produce #{inspect aSource.id}"
    	prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
        message = %Message{
            source: aSource.id,
            sender: aSource.id,
            value: aSource.fct.(),
            timestamp: :os.system_time(:microsecond),
            chckptId: aSource.chckptId
        }
        #IO.puts inspect aSource.repNodes
        #Utils.poolCall(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
        Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
    	Enum.map(aSource.children, &Signal.propagate(Map.get(aSource.routing, &1), &1, message))
    	{ :noreply, %{ aSource | prodTimer: prodTimer } }
    end

    def handle_info({:EXIT, _pid, :normal}, s) do
        { :noreply, s }
    end

    def handle_info(:checkpoint, aSource) do
        chckptTimer = Process.send_after(aSource.id, :checkpoint, aSource.chckptInterval)
        message = %ChckptMessage{ 
            id: aSource.chckptId,
            sender: aSource.id 
        }
        Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
        Enum.map(aSource.children, &Signal.propagateChckpt(Map.get(aSource.routing, &1), &1, message))
        { :noreply, %{ aSource |
            chckptTimer: chckptTimer,
            chckptId: aSource.chckptId+1
        } }
    end

    def handle_info(:start, aSource) do
    	#IO.puts "start #{inspect aSource.id}"
        aSource = %{ aSource | startReceived: aSource.startReceived+1 }
        if length(aSource.children)==aSource.startReceived do
            prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
            chckptTimer = Process.send_after(aSource.id, :checkpoint, aSource.chckptInterval)
            { :noreply, %{ aSource | 
                prodTimer: prodTimer,
                chckptTimer: chckptTimer
            } }
        else
        	{ :noreply, aSource }
        end
    end

end