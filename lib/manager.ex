defmodule Manager do
    use GenServer

    # Client API

    def start_link(_args, _opts) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def normalExit(pid) do
        GenServer.cast(__MODULE__, {:normalExit, pid})
    end

    # Server API

    def init(:ok) do
        {:ok, nil}
    end

    def handle_cast({:normalExit, pid}, state) do
        Process.monitor(pid)
        Supervisor.terminate_child(DNode.Supervisor, pid)
        receive do
            {:DOWN, _ref, :process, ^pid, :shutdown} -> :ok
        after 
            1000 -> Process.exit(pid, :kill)
        end
        if Supervisor.count_children(DNode.Supervisor).workers==0 do
            Supervisor.stop(Drepel.Supervisor)
        end
        {:noreply, state}
    end

end