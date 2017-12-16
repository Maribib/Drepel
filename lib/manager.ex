defmodule Node.Supervisor do 

	def run(nodeName) do
		Node.monitor(nodeName, true)
		receive do
			{:nodedown, ^nodeName} -> 
				Enum.map(Supervisor.which_children(Source.Supervisor), fn {_, pid, _, _} ->
					Supervisor.terminate_child(Source.Supervisor, pid)
				end)
				Enum.map(Supervisor.which_children(Signal.Supervisor), fn {_, pid, _, _} ->
					Supervisor.terminate_child(Signal.Supervisor, pid)
				end)
		end
	end
end