require Logger

defmodule ClusterSupervisor do 
	use GenServer

	def _stop do
		# stop balancer
		#Balancer.stop()
		# stop checkpointing
		Checkpoint.stop()
		# stop sampling
		#Sampler.stop()
		# stop nodes (sources and signals)
		supervisors = [Source.Supervisor, Signal.Supervisor]
		Enum.map(supervisors, &Utils.stopChildren(&1))
	end

 	# Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def monitor(nodes) do
    	Utils.multi_call(nodes, __MODULE__, {:monitor, nodes})
    end

    def monitor(aNode, clustNodes) do
		GenServer.call({__MODULE__, aNode}, {:monitor, clustNodes})
    end

    # Server API

    def init(:ok) do
        { :ok, [] }
    end

	def handle_call({:monitor, newClustNodes}, _from, clustNodes) do
		Enum.map(clustNodes -- newClustNodes, &Node.monitor(&1, false))
		Enum.map(newClustNodes -- clustNodes, &Node.monitor(&1, true))
		{ :reply, :ok, newClustNodes }
	end

	def handle_call({:stop, newClustNodes}, _from, _clustNodes) do
		_stop()
		{:reply, :ok, newClustNodes}
	end

	def handle_info({:nodedown, nodeName}, clustNodes) do
		newClustNodes = clustNodes -- [nodeName]
		Enum.map(clustNodes -- newClustNodes, &Node.monitor(&1, false))
		if node()==Enum.at(newClustNodes, 0) do
			# stop locally
			_stop()
			# stop remote
			{_, bad_nodes} = GenServer.multi_call(newClustNodes -- [node()], __MODULE__, {:stop, newClustNodes})
			if length(bad_nodes)<1 do # no other failure do
			 	Checkpoint.lastCompleted()
				|> Drepel.Env.restore(newClustNodes)
			end
		end
		{:noreply, newClustNodes }
	end

end