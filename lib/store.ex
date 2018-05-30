defmodule Store do
	defstruct [ repNodes: [], ids: [] ]

	# Transactions
	# The transaction handler ensures that either 
	# all operations in the transaction are performed 
	# successfully on all nodes atomically, or the 
	# transaction fails without permanent effect on any node.

    use GenServer
    
    # Client API

    def start(clustNodes) do
    	:mnesia.create_schema(clustNodes)
    	Utils.multi_call(clustNodes--[node()], __MODULE__, :start)
    	handle_call(:start, nil, nil)
    end

    def createTables(env) do
    	tableInfos = Enum.reduce(env.clustNodes, %{}, fn node, acc ->
            repNodes = Drepel.Env.computeRepNodes(env, env.clustNodes, node)
            ids = Enum.filter(env.routing, &(elem(&1,1)==node))
            |> Enum.map(&elem(&1,0))
            Map.put(acc, node, {repNodes, ids})
        end)
    	req = {:createTables, tableInfos}
    	Utils.multi_call(env.clustNodes, __MODULE__, req)
    end

    def handle_call({:createTables, tableInfos}, _from, state) do
    	{repNodes, ids} = Map.get(tableInfos, node())
    	Enum.map(ids, fn id ->
	    	:mnesia.create_table(id, [
	    		attributes: [:chckptId, :value],
	    		type: :bag,
	    		ram_copies: repNodes
	    	])
	    end)
    	{ :reply, :ok, %{ state | ids: ids } }
    end

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodes) do
    	:mnesia.delete_schema(nodes)
    end

    def setRepNodes(nodes) do
    	GenServer.call(__MODULE__, {:setRepNodes, nodes})
    end

    def put(chckptId, message) do
    	GenServer.call(__MODULE__, {:put, chckptId, message})
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
        { :ok, %__MODULE__{} }
    end

    def handle_call(:start, _from, state) do
    	:mnesia.start()
    	{ :reply, :ok, state }
    end

    def handle_call({:put, chckptId, %{sender: sender}=message}, _from, state) do
    	res = :mnesia.transaction(fn ->
	    	:mnesia.write({sender, chckptId, message})
	    end)
	    { :reply, res, state }
    end

    def handle_call({:put, chckptId, %Signal{id: id}=signal}, _from, state) do
    	res = :mnesia.transaction(fn ->
	    	:mnesia.write({id, chckptId, signal})
	    end)
	    { :reply, res, state }
    end

    def handle_call({:get, id, chckptId}, _from, state) do
    	res = :mnesia.transaction(fn ->
    		:mnesia.match_object({id, chckptId, :_})
    	end)
    	|> elem(1)
    	|> List.last()
		|> elem(2)
    	{ :reply, res, state }
    end

    def handle_call({:getMessages, id, chckptId}, _from, state) do
    	res = :mnesia.transaction(fn ->
    		:mnesia.select(id, 
				[
					{
						{id, :"$1", :"$2"}, 
						[{:>=, :"$1", chckptId}],
						[:"$2"]
					}
				]
	    	)
    	end)
    	{ :reply, elem(res, 1), state }
    end	

    def handle_call({:setRepNodes, nodes}, _from, state) do
    	#newRepNodes = nodes -- state.repNodes
		#Enum.map(newRepNodes, fn repNode ->
		#	x = :mnesia.add_table_copy(Message, repNode, :ram_copies)
		#	IO.puts inspect x
		#	y = :mnesia.add_table_copy(Signal, repNode, :ram_copies)
		#	IO.puts inspect y
		#end)
    	{ :reply, :ok, state}
    end

    def handle_cast({:clean, chckptId}, state) do
    	:mnesia.transaction(fn ->
	    	Enum.map(state.ids, fn id -> 
	    		:mnesia.select(id, 
					[
						{
							{id, :"$1", :"$2"}, 
							[{:<, :"$1", chckptId}],
							[:"$_"]
						}
					]
		    	)
		    	|> Enum.map(fn record ->
		    		:mnesia.delete_object(record)
		    	end)
		    end)
    	end)
    	{ :noreply, state }
    end

end