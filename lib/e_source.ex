require Logger

defmodule ESource do
    @enforce_keys [ :id, :name, :default ]
    defstruct [ :id, :default, :dependencies, :name, :server,
    children: [], startReceived: 0, repNodes: [],
    chckptId: 0, routing: %{}, socket: nil]
    
    use GenServer, restart: :transient

    def propagate(aSource, msg) do
        Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagate(node, id, msg)
        end)
    end

    # Client API

    def start_link({aSource, messages}) do
        GenServer.start_link(__MODULE__, {aSource, messages}, name: aSource.id)
    end

    def start_link(aSource) do 
        GenServer.start_link(__MODULE__, aSource, name: aSource.id)
    end

    def socket(id, sock) do
        GenServer.call(id, {:socket, sock})
    end

    # Server API

    def init(%__MODULE__{id: sid}=aSource) do
        Process.flag(:trap_exit, true)
        name = String.to_atom("tcp_#{Atom.to_string(aSource.id)}")
        Enum.map(aSource.children, fn id ->
            node = Map.get(aSource.routing, id)
            Signal.propagateDefault(node, id, sid, aSource.default)
        end)
        { :ok, %{ aSource | server: name } }
    end

    def init({%__MODULE__{}=aSource, messages}) do
        Process.flag(:trap_exit, true)
        chckptId = Enum.reduce(messages, aSource.chckptId, fn msg, acc ->
            chckptId = case msg do
                %ChckptMessage{id: id} -> id+1
                %Message{} -> acc
            end
            propagate(aSource, msg)
            chckptId
        end)
        name = String.to_atom("tcp_#{Atom.to_string(aSource.id)}")
        {:ok, %{ aSource |
            chckptId: chckptId,
            server: name
        } }
    end

    def handle_call({:socket, sock}, _from, aSource) do
        Port.connect(sock, self())
        { :reply, :ok, %{ aSource | socket: sock } }
    end

    def handle_cast({:checkpoint, chckptId}, aSource) do
        if chckptId>aSource.chckptId do
            msg = %ChckptMessage{ 
                id: chckptId,
                sender: aSource.id 
            }
            Store.put(aSource.repNodes, chckptId-1, msg)
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
            { :noreply, aSource }
        else
            { :noreply, aSource }
        end
    end

    def handle_info({:tcp, socket, value}, aSource) do
        msg = %Message{
            source: aSource.id,
            sender: aSource.id,
            value: value,
            chckptId: aSource.chckptId
        }
        Store.put(aSource.repNodes, aSource.chckptId, msg)
        :gen_tcp.send(socket, "ack")
        propagate(aSource, msg)    
        {:noreply, aSource}
    end

    def handle_info({:tcp_closed, _socket}, aSource) do
        Logger.info("tcp closed")
        { :noreply, %{ aSource | socket: nil } }
    end

    def handle_info({:tcp_error, _socket, reason}, aSource) do
        Logger.info("tcp error #{inspect reason}")
        { :noreply, %{ aSource | socket: nil } }
    end

    def terminate(_reason, aSource) do
        Utils.closeSocket(aSource)
    end
end
