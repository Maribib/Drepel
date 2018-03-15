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
end