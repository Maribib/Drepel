defmodule Store do

    use GenServer

    def computeRepNodes(clustNodes, repFactor) do
	    computeRepNodes(clustNodes, repFactor, node())
	end

	def computeRepNodes(clustNodes, repFactor, aNode) do
		nbNodes = length(clustNodes)
		pos = Enum.find_index(clustNodes, fn clustNode -> clustNode==aNode end)
	    Enum.map(pos..pos+repFactor, fn i -> Enum.at(clustNodes, rem(i, nbNodes)) end)
	end
    
    # Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodeName) do
    	GenServer.call({__MODULE__, nodeName}, :reset)
    end

    def put(nodeName, chckptId, message) do
    	GenServer.call({__MODULE__, nodeName}, {:put, chckptId, message})
    end

    def get(id, chckptId) do
    	GenServer.call(__MODULE__, {:get, id, chckptId})
    end

    def clean(nodeName, chckptId) do
    	GenServer.cast({__MODULE__, nodeName}, {:clean, chckptId})
    end

    def getMessages(id, chckptId) do
        GenServer.call(__MODULE__, {:getMessages, id, chckptId})
    end


    # Server API

    def init(:ok) do
        { :ok, %{} }
    end

    def handle_call(:reset, _from, _store) do
    	{ :reply, :ok, %{} }
    end

    def handle_call({:put, chckptId, %Message{source: source}=message}, _from, store) do
    	chckptStore = Map.get(store, chckptId, %{})
    	messages = Map.get(chckptStore, source, [])
    	{ :reply, :ok,
    		Map.put(store, chckptId,
    			Map.put(chckptStore, source, messages ++ [message])
    		)
    	}
    end

    def handle_call({:put, chckptId, %Signal{id: id}=signal}, _from, store) do
    	chckptStore = Map.get(store, chckptId, %{})
    	{ :reply, :ok,
    		Map.put(store, chckptId, 
    			Map.put(chckptStore, id, signal)
    		)
    	}
    end

    def handle_call({:get, id, chckptId}, _from, store) do
    	{ :reply, get_in(store, [chckptId, id]), store }
    end

    def handle_call({:getMessages, id, chckptId}, _from, store) do
    	messages = Enum.sort(Map.keys(store)) 
    	|> Enum.filter(&(&1>=chckptId))
    	|> Enum.reduce([], fn chckptId, acc -> 
    		chckpt = Map.get(store, chckptId)
    		if Map.has_key?(chckpt, id) do
    			acc ++ Map.get(chckpt, id)
    		else
    			acc
    		end
    	end)
    	{ :reply, messages, store }
    end	

    def handle_cast({:clean, chckptId}, store) do
    	{ :noreply, Map.delete(store, chckptId)}
    end

end