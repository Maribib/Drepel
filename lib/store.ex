defmodule Store do
	defstruct [ repNodes: [], ids: [] ]

	# Transactions
	# The transaction handler ensures that either 
	# all operations in the transaction are performed 
	# successfully on all nodes atomically, or the 
	# transaction fails without permanent effect on any node.

    use GenServer
    
    # Client API

    def start do
    	:mnesia.start()
    end

    def start(clustNodes) do
    	Utils.multi_call(clustNodes, __MODULE__, :start)
    	:mnesia.change_config(:extra_db_nodes, clustNodes)
    end

    def createTables(clustNodes, tableInfos) do
    	Utils.multi_call(clustNodes, __MODULE__, {:createTables, tableInfos})
    end

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodes) do
    	:mnesia.delete_schema(nodes)
    end

    def updateRepNodes(clustNodes, tableInfos) do
        GenServer.multi_call(clustNodes, Store, {:updateRepNodes, tableInfos})
    end

    def _put(chckptId, %{sender: sender}=message) do
    	:mnesia.transaction(fn ->
	    	:mnesia.write({sender, chckptId, message})
	    end)
    end

    def _put(chckptId, %Signal{id: id}=signal) do
  		:mnesia.transaction(fn ->
	    	:mnesia.write({id, chckptId, signal})
	    end)
    end

    def put(chckptId, struct) do
    	res = _put(chckptId, struct)
    	case res do
	    	{:aborted, _} -> exit(:failed_transaction)
	    	{:atomic, _} -> :ok
	    end
    end

    def get(id, chckptId) do
    	:mnesia.transaction(fn ->
    		:mnesia.match_object({id, chckptId, :_})
    	end)
    	|> elem(1)
    	|> List.last()
		|> elem(2)
    end

    def clean(nodeNames, chckptId) do
    	Enum.map(nodeNames, &GenServer.cast({__MODULE__, &1}, {:clean, chckptId}))
    end

    def getMessages(id, chckptId) do
    	:mnesia.transaction(fn ->
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
    	|> elem(1)
    end

    def replicate(from, to, ids) do
    	GenServer.call({__MODULE__, from}, {:replicate, to, ids})
    end

    def discover(clustNode) do
    	:mnesia.change_config(:extra_db_nodes, [clustNode])
    end

    def move(env, from, to, ids) do
    	oldRepNodes = Drepel.Env.computeRepNodes(env, env.clustNodes, from)
        newRepNodes = Drepel.Env.computeRepNodes(env, env.clustNodes, to)
        toAdd = newRepNodes -- oldRepNodes
        toDel = oldRepNodes -- newRepNodes
        Enum.each(ids, fn id ->
            Enum.each(toAdd, fn node -> 
                :mnesia.add_table_copy(id, node, :ram_copies)
            end)
            Enum.each(toDel, fn node -> 
                :mnesia.del_table_copy(id, node)
            end)
        end)
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call(:start, _from, state) do
    	:mnesia.start()
    	{ :reply, :ok, state }
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
    	{ :reply, :ok, %{ state | ids: ids, repNodes: repNodes } }
    end

    def handle_call({:updateRepNodes, tableInfos}, _from, state) do
    	{repNodes, ids} = Map.get(tableInfos, node())
    	Enum.each(ids, fn id ->
    		Enum.each(repNodes, fn repNode ->
				:mnesia.add_table_copy(id, repNode, :ram_copies)
    		end)
    	end)
    	{ :reply, :ok, %{ state | repNodes: repNodes, ids: ids } }
    end

    def handle_call({:replicate, to, ids}, _, state) do
    	Enum.map(ids, fn id ->
    		:mnesia.add_table_copy(id, to, :ram_copies)
    	end)
    	{ :reply, :ok, state }
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