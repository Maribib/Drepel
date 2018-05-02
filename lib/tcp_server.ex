require Logger

defmodule TCPServer do
    @enforce_keys [ :id, :port, ]
    defstruct [ :id, :port, :socket, :listenSocket]

    @ip Application.get_env(:drepel, :tcp_ip)
    
    use GenServer, restart: :transient

    # Client API

    def start_link(_opts, {name, id, port}) do 
        GenServer.start_link(
            __MODULE__, 
            %__MODULE__{
                id: id,
                port: port
            },
            name: name
        )
    end

    def init(state) do
        Process.flag(:trap_exit, true)
        { :ok, state }
    end

    def close(nil), do: nil
    def close(socket), do: :gen_tcp.close(socket)

    def terminate(_reason, state) do
        close(state.socket)
        close(state.listenSocket)
    end

    def accept(id) do
        GenServer.cast(id, :accept)
    end

    # Server API

    def handle_cast(:accept, state) do
        {:ok, listenSocket} = :gen_tcp.listen(
            state.port, 
            [
                :binary, 
                packet: 4, 
                active: true, 
                ip: @ip,
                reuseaddr: true
            ]
        )
        {:ok, socket } = :gen_tcp.accept listenSocket
        { :noreply, %{ state | socket: socket, listenSocket: listenSocket } }
    end

    def handle_info({:tcp, socket, value}, state) do
        EventSource.onReceive(state.id, value)
        :gen_tcp.send(socket, "ack")
        {:noreply, state}
    end

    def handle_info({:tcp_closed, _socket}, state) do
        Logger.info("tcp closed")
        {:ok, socket } = :gen_tcp.accept state.listenSocket
        { :noreply, %{ state | socket: socket } }
    end

    def handle_info({:tcp_error, _socket, reason}, state) do
        Logger.info("tcp error #{inspect reason}")
        {:ok, socket } = :gen_tcp.accept state.listenSocket
        { :noreply, %{ state | socket: socket } }
    end
end