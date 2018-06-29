require Logger

defmodule MyStuff do
    use GenServer

    defstruct [ nameToId: [] ]

    # Client API

    def start_link(_opts) do 
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(nodeToName) do 
        group = Enum.group_by(nodeToName, &elem(&1, 0), &elem(&1, 1))
        Enum.map(group, fn {node, idAndName} ->
            GenServer.call({__MODULE__, node}, {:reset, idAndName})
        end)
    end

    def socket(sock) do
        GenServer.call(__MODULE__, {:socket, sock})
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, idAndName}, _from, state) do 
        nameToId = Enum.reduce(idAndName, %{}, fn {id, name}, acc ->
            Map.put(acc, name, id)
        end)
        { :reply, :ok, %{ state | nameToId: nameToId } }
    end

    def handle_call({:socket, socket}, _from, state) do
        Port.connect(socket, self())
        :gen_tcp.send(socket, "name")
        { :reply, :ok, state }
    end

    def handle_info({:tcp, socket, name}, state) do
        res = Map.get(state.nameToId, name, :err)
        case res do
            :err -> :gen_tcp.close(socket)
            id -> 
                ESource.socket(id, socket)
                Process.unlink(socket)
        end
        {:noreply, state}
    end

end

defmodule TCPServer do
    defstruct [ :listenSocket, idToName: [] ]

    @ip Application.get_env(:drepel, :tcp_ip)
    @port Application.get_env(:drepel, :tcp_port)
    
    use GenServer

    # Client API

    def start_link(_opts) do 
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def close(nil), do: nil
    def close(socket), do: :gen_tcp.close(socket)

    def terminate(_reason, state) do
        Utils.closeSocket(state.listenSocket)
    end

    def accept do
        GenServer.cast(__MODULE__, :accept)
    end

    # Server API

    def init(:ok) do
        Process.flag(:trap_exit, true)
        try do
            {:ok, listenSocket} = :gen_tcp.listen(
                @port, 
                [
                    :binary, 
                    packet: 0, 
                    active: true, 
                    ip: @ip,
                    reuseaddr: true
                ]
            )
            accept()
            { :ok, %__MODULE__{ listenSocket: listenSocket } }
        rescue
            MatchError -> 
                Logger.error("Could not start tpc server on port #{@port}")
                { :ok, %__MODULE__{} }
        end
    end 


    def handle_cast(:accept, state) do
        {:ok, socket} = :gen_tcp.accept state.listenSocket
        MyStuff.socket(socket)
        Process.unlink(socket)
        accept()
        { :noreply, state }
    end

end