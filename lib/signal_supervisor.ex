require Signal

defmodule Signal.Supervisor do
    use Supervisor

    def start_link(_opts \\ nil) do
        Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def start(%Signal{}=aSignal) do
        Supervisor.start_child(__MODULE__, [aSignal])
    end
    
    def restart(id, chckptId, routing, repNodes, leader) do
        aSignal = Store.get(id, chckptId)
        aSignal = %{ aSignal | 
            repNodes: repNodes,
            routing: routing, 
            leader: leader
        }
        Supervisor.start_child(__MODULE__, [aSignal])
    end

    def init(:ok) do
        Supervisor.init([Signal], strategy: :simple_one_for_one)
    end

end