defmodule Node.Supervisor do 
	defstruct [clustNodes: [], stopMessages: [], nodesDown: [] ]

	use GenServer

 	# Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def monitor(aNode, clustNodes) do
		GenServer.call({__MODULE__, aNode}, {:monitor, clustNodes})
    end

    def stopChildren(aSupervisor) do
    	Enum.map(Supervisor.which_children(aSupervisor), fn {_, pid, _, _} ->
			Supervisor.terminate_child(aSupervisor, pid)
		end)
    end

    def stop(aNode) do
    	GenServer.cast({__MODULE__ ,aNode}, {:stop, node()})
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

	def handle_cast({:stop, aNode}, state) do
		stopMessages = state.stopMessages ++ [aNode]
		if length(state.clustNodes--stopMessages)==0 do
			chckptId = Checkpoint.lastCompleted()
			Drepel.Env.restore(chckptId, state.clustNodes, state.nodesDown)
			{ :noreply, %{ state | stopMessages: []} }
		else
			{ :noreply, %{ state | stopMessages: stopMessages} }
		end
	end

	def handle_info({:nodedown, nodeName}, state) do
		Balancer.stop()
		# Stop stats sampling
		Drepel.Stats.stopSampling()
		# Stop nodes (sources and signals)
		supervisors = [Source.Supervisor, EventSource.Supervisor, Signal.Supervisor]
		Enum.map(supervisors, &stopChildren(&1))
		# Elect and alert new leader
		clustNodes = state.clustNodes -- [nodeName]
		leader = Enum.at(clustNodes, 0)
		stop(leader)
		{:noreply, %{ state | 
			clustNodes: clustNodes, 
			nodesDown: state.nodesDown ++ [nodeName]
		} }
	end

end