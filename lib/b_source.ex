require Signal

defmodule BSource do 
	@enforce_keys [ :id, :refreshRate, :fct, :default ]
	defstruct [ :id, :refreshRate, :fct, :default, 
    :dependencies, :clustNodes,
	children: [], startReceived: 0, prodTimer: nil,
    chckptId: 0, routing: %{}]

	use GenServer, restart: :transient

    def propagate(aSource, msg) do
        Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagate(node, id, msg)
        end)
    end

	# Client API

    def start_link({aSource, messages}) do
        GenServer.start_link(__MODULE__, {aSource, messages}, name: aSource.id)
    end

    def start_link(aSource) do
        GenServer.start_link(__MODULE__, aSource, name: aSource.id)
    end

	# Server API

	def init(%__MODULE__{}=aSource) do
		Process.flag(:trap_exit, true)
		defaultValue = case aSource.default do
			d when is_function(d) -> d.()
			_ -> aSource.default
		end
		Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagateDefault(node, id, aSource.id, defaultValue)
        end)
        { :ok, aSource }
    end

    def init({%__MODULE__{}=aSource, messages}) do
        Process.flag(:trap_exit, true)
        chckptId = Enum.reduce(messages, aSource.chckptId, fn msg, acc ->
            chckptId = case msg do
                %ChckptMessage{id: id} -> id+1
                %Message{} -> acc
            end
            propagate(aSource, msg)
            chckptId
        end)
        {:ok, %{ aSource |
            prodTimer: Process.send_after(aSource.id, :produce, aSource.refreshRate),
            chckptId: chckptId
        } }
    end

    def terminate(_reason, aSource) do
        Utils.cancelTimer(aSource.prodTimer)
    end

    def handle_info(:produce, aSource) do
    	prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
        msg = %Message{
            source: aSource.id,
            sender: aSource.id,
            value: aSource.fct.(),
            chckptId: aSource.chckptId
        }
        Store.put(aSource.chckptId, msg)
    	propagate(aSource, msg)
    	{ :noreply, %{ aSource | prodTimer: prodTimer } }
    end

    def handle_info({:EXIT, _pid, :normal}, s) do
        { :noreply, s }
    end

    def handle_info(:start, aSource) do
    	#IO.puts "start #{inspect aSource.id}"
        aSource = %{ aSource | startReceived: aSource.startReceived+1 }
        if length(aSource.children)==aSource.startReceived do
            prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
            { :noreply, %{ aSource | 
                prodTimer: prodTimer,
            } }
        else
        	{ :noreply, aSource }
        end
    end

    def handle_cast({:checkpoint, chckptId}, aSource) do
        if chckptId>aSource.chckptId do
            msg = %ChckptMessage{ 
                id: chckptId,
                sender: aSource.id 
            }
            Store.put(chckptId-1, msg)
            propagate(aSource, msg)
            { :noreply, %{ aSource |
                chckptId: chckptId
            } }
        else
            { :noreply, aSource }
        end
    end

end