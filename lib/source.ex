require Signal

defmodule Source do 
	@enforce_keys [ :id, :refreshRate, :fct, :default ]
	defstruct [ :id, :refreshRate, :fct, :default, :dependencies, :chckptInterval, 
	children: [], startReceived: 0, prodTimer: nil, repNodes: [], chckptTimer: nil,
    chckptId: 0, routing: %{}]

	use GenServer, restart: :transient

	# Client API

	def start_link(_opts, aSource, clustNodes, repFactor, chckptInterval) do
		GenServer.start_link(__MODULE__, {aSource, clustNodes, repFactor, chckptInterval}, name: aSource.id)
    end

    def start_link(_opts, aSource, clustNodes, repFactor, chckptInterval, messages) do
        GenServer.start_link(__MODULE__, {aSource, clustNodes, repFactor, chckptInterval, messages}, name: aSource.id)
    end

	# Server API

	def init({%__MODULE__{}=aSource, clustNodes, repFactor, chckptInterval}) do
		Process.flag(:trap_exit, true)
		defaultValue = case aSource.default do
			d when is_function(d) -> d.()
			_ -> aSource.default
		end
		Enum.map(aSource.children, &Signal.propagateDefault(Map.get(aSource.routing, &1), &1, aSource.id, defaultValue))
        { :ok, %{ aSource | 
            repNodes: [node()] ++ Store.computeRepNodes(clustNodes, repFactor), 
            chckptInterval: chckptInterval 
        } }
    end

    def init({%__MODULE__{}=aSource, clustNodes, repFactor, chckptInterval, messages}) do
        IO.puts "restarted"
        Process.flag(:trap_exit, true)
        prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
        chckptTimer = Process.send_after(aSource.id, :checkpoint, chckptInterval)
        Enum.map(messages, fn message ->
            Enum.map(aSource.children, &Signal.propagate(Map.get(aSource.routing, &1), &1, message))
        end)
        {:ok, %{ aSource |
            repNodes: [node()] ++ Store.computeRepNodes(clustNodes, repFactor), 
            prodTimer: prodTimer,
            chckptTimer: chckptTimer,
            chckptInterval: chckptInterval
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
        Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
    	Enum.map(aSource.children, &Signal.propagate(Map.get(aSource.routing, &1), &1, message))
    	{ :noreply, %{ aSource | prodTimer: prodTimer } }
    end

    def handle_info(:checkpoint, aSource) do
        chckptTimer = Process.send_after(aSource.id, :checkpoint, aSource.chckptInterval)
        message = %ChckptMessage{ 
            id: aSource.chckptId,
            sender: aSource.id 
        }
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