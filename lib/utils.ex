require Logger

defmodule Utils do
	def cancelTimer(nil) do
        nil
    end

    def cancelTimer(timer) do
        Process.cancel_timer(timer)
    end

    def closeSocket(nil) do
    	nil
    end

    def closeSocket(socket) do
        :gen_tcp.close(socket)
    end

    def stopChildren(supervisor) do
    	Enum.map(Supervisor.which_children(supervisor), fn {_, pid, _, _} ->
			Supervisor.terminate_child(supervisor, pid)
		end)
    end

    def multi_call(nodes, name, msg) do
    	{_, bad_nodes} = GenServer.multi_call(nodes, name, msg)
		if length(bad_nodes)>0 do
			Logger.error("no response: #{inspect bad_nodes}")
			exit(:connexion_lost)
		end
    end
end