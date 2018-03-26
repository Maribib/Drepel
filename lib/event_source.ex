defmodule EventSource do
    @enforce_keys [ :id, :port, :default ]
    defstruct [ :id, :default, :dependencies, :port, :server,
    children: [], startReceived: 0, repNodes: [],
    chckptId: 0, routing: %{}]
    
    use GenServer, restart: :transient

    def propagate(aSource, msg) do
        Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagate(node, id, msg)
        end)
    end

    # Client API

    def start_link(_opts, aSource) do 
        GenServer.start_link(__MODULE__, aSource, name: aSource.id)
    end

    def start_link(_opts, aSource, messages) do
        GenServer.start_link(__MODULE__, {aSource, messages}, name: aSource.id)
    end

    def init(%__MODULE__{id: sid}=aSource) do
        name = String.to_atom("tcp_#{Atom.to_string(aSource.id)}")
        IO.puts inspect TCPServer.Supervisor.start({name, aSource.id, aSource.port})
        Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagateDefault(node, id, sid, aSource.default)
        end)
        { :ok, %{ aSource | server: name } }
    end

     def init({%__MODULE__{}=aSource, messages}) do
        chckptId = Enum.reduce(messages, aSource.chckptId, fn msg, acc ->
            chckptId = case msg do
                %ChckptMessage{id: id} -> id+1
                %Message{} -> acc
            end
            propagate(aSource, msg)
            chckptId
        end)
        name = String.to_atom("tcp_#{Atom.to_string(aSource.id)}")
        TCPServer.Supervisor.start({name, aSource.id, aSource.port})
        TCPServer.accept(aSource.server)
        {:ok, %{ aSource |
            chckptId: chckptId,
            server: name
        } }
    end

    def onReceive(id, value) do
        GenServer.call(id, {:onReceive, value})
    end

    # Server API

    def handle_call({:onReceive, value}, _from, aSource) do
        msg = %Message{
            source: aSource.id,
            sender: aSource.id,
            value: value,
            chckptId: aSource.chckptId
        }
        Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, msg))
        propagate(aSource, msg)
        { :reply, :ok, aSource }
    end

    def handle_cast({:checkpoint, chckptId}, aSource) do
        if chckptId>aSource.chckptId do
            msg = %ChckptMessage{ 
                id: chckptId,
                sender: aSource.id 
            }
            Enum.map(aSource.repNodes, &Store.put(&1, chckptId-1, msg))
            propagate(aSource, msg)
            { :noreply, %{ aSource |
                chckptId: chckptId
            } }
        else
          { :noreply, aSource }
        end
    end

    def handle_info(:start, aSource) do
        aSource = %{ aSource | startReceived: aSource.startReceived+1 }
        if length(aSource.children)==aSource.startReceived do
            TCPServer.accept(aSource.server)
            { :noreply, aSource }
        else
            { :noreply, aSource }
        end
    end

end
