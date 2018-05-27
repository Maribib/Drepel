require Logger

defmodule ClusterSupervisor do 
	defstruct [clustNodes: [], stopMessages: [], nodesDown: [] ]

	use GenServer

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

    def stop(aNode, nodeDown) do
    	GenServer.cast({__MODULE__ ,aNode}, {:stop, node(), nodeDown})
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

	def handle_call({:monitor, clustNodes}, _from, state) do
		Enum.map(state.clustNodes -- clustNodes, &Node.monitor(&1, false))
		Enum.map(clustNodes -- state.clustNodes, &Node.monitor(&1, true))
		{ :reply, :ok, %{ state | clustNodes: clustNodes } }
	end

	def handle_cast({:stop, aNode, nodeDown}, state) do
		stopMessages = state.stopMessages ++ [aNode]
		nodesDown = Enum.uniq(state.nodesDown ++ [nodeDown])
		if length(state.clustNodes--stopMessages)==0 do
			chckptId = Checkpoint.lastCompleted()
			Drepel.Env.restore(chckptId, state.clustNodes, nodesDown)
			{ :noreply, %{ state | 
				stopMessages: [], 
				nodesDown: []
			} }
		else
			{ :noreply, %{ state | 
				stopMessages: stopMessages, 
				nodesDown: nodesDown
			} }
		end
	end

	def handle_info({:nodedown, nodeName}, state) do
		Logger.error("node down: #{nodeName}")
		# stop balancer
		Balancer.stop()
		# stop checkpointing
		Checkpoint.stop()
		# stop sampling
		Sampler.stop()
		# stop nodes (sources and signals)
		supervisors = [Source.Supervisor, Signal.Supervisor]
		Enum.map(supervisors, &Utils.stopChildren(&1))
		# Elect and alert new leader with stop message
		clustNodes = state.clustNodes -- [nodeName]
		leader = Enum.at(clustNodes, 0)
		stop(leader, nodeName)
		{:noreply, %{ state | 
			clustNodes: clustNodes
		} }
	end

end