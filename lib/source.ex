require Signal

defmodule Source do 
	@enforce_keys [ :id, :refreshRate, :fct, :default ]
	defstruct [ :id, :refreshRate, :fct, :default, :dependencies, 
    :clustNodes, :repNodes,
	children: [], startReceived: 0, prodTimer: nil, repNodes: [],
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
        Process.flag(:trap_exit, true)
        chckptId = Enum.reduce(messages, aSource.chckptId, fn message, acc ->
            {propFct, chckptId} = case message do
                %ChckptMessage{id: id} -> 
                    IO.puts "replayed #{id}"
                    {&Signal.propagateChckpt/3, id+1}
                %Message{} -> {&Signal.propagate/3, acc}
            end
            Enum.map(aSource.children, &propFct.(Map.get(aSource.routing, &1), &1, message))
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
    	#IO.puts "produce #{inspect aSource.id}"
    	prodTimer = Process.send_after(aSource.id, :produce, aSource.refreshRate)
        message = %Message{
            source: aSource.id,
            sender: aSource.id,
            value: aSource.fct.(),
            chckptId: aSource.chckptId
        }
        #Utils.poolCall(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
        Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
    	Enum.map(aSource.children, &Signal.propagate(Map.get(aSource.routing, &1), &1, message))
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
            message = %ChckptMessage{ 
                id: chckptId,
                sender: aSource.id 
            }
            Enum.map(aSource.repNodes, &Store.put(&1, chckptId-1, message))
            Enum.map(aSource.children, &Signal.propagateChckpt(Map.get(aSource.routing, &1), &1, message))
            { :noreply, %{ aSource |
                chckptId: chckptId
            } }
        else
            { :noreply, aSource }
        end
    end

    def handle_call({:addRepNode, node}, _from, aSource) do 
        if Enum.member?(aSource.repNodes, node) do
            { :reply, :already, aSource }
        else
            { :reply, :ok, update_in(aSource.repNodes, &(&1 ++ [node])) }
        end
    end
end