require Logger

defmodule Utils do
	def cancelTimer(nil) do
        nil
    end

    def cancelTimer(timer) do
        Process.cancel_timer(timer)
    end

    def stopChildren(supervisor) do
    	Enum.map(Supervisor.which_children(supervisor), fn {_, pid, _, _} ->
			Supervisor.terminate_child(supervisor, pid)
		end)
    end

    def stopChildren(supervisor, nodes) do
        Enum.map(nodes, fn node ->
            Task.Supervisor.async({Task.Spawner, node}, fn ->
                Supervisor.which_children(supervisor)
                |> Enum.map(&Supervisor.terminate_child(supervisor, elem(&1, 1)))
            end)
        end)
        |> Enum.map(&Task.await(&1))
    end

    def multi_call(nodes, name, msg) do
    	{_, bad_nodes} = GenServer.multi_call(nodes, name, msg)
		if length(bad_nodes)>0 do
			Logger.error("no response: #{inspect bad_nodes}")
			exit(:connexion_lost)
		end
    end
end