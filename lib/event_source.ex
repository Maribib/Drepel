require Source

defmodule EventSource do
  @enforce_keys [ :id, :port, :default ]
  defstruct [ :id, :port, :default, :dependencies, :socket, :listen_socket,
  children: [], startReceived: 0, ip: {127,0,0,1} ]
  
  use GenServer, restart: :transient

  # Client API

  def start_link(_opts, aSource) do 
    {id, _node} = aSource.id
    GenServer.start_link(__MODULE__, aSource, name: id)
  end

  def init(%__MODULE__{}=aSource) do
    Process.flag(:trap_exit, true)
    Enum.map(aSource.children, &Signal.propagateDefault(&1, aSource.id, aSource.default))
    {:ok, aSource}
  end

  def terminate(_reason, aSource) do
      :gen_tcp.close(aSource.socket)
      :gen_tcp.close(aSource.listen_socket)
  end

  # Server API

  def handle_info({:tcp, _socket, value}, aSource) do
    Enum.map(aSource.children, &Signal.propagate(&1, aSource.id, aSource.id, value))
    {:noreply, aSource}
  end

  def handle_info({:tcp_closed, _socket}, aSource) do
    IO.puts "tcp closed"
    {:ok, socket } = :gen_tcp.accept aSource.listen_socket
    { :noreply, %{ aSource | socket: socket } }
  end

  def handle_info({:tcp_error, _socket, reason}, aSource) do
    IO.puts "tcp error #{inspect reason}"
    {:ok, socket } = :gen_tcp.accept aSource.listen_socket
    { :noreply, %{ aSource | socket: socket } }
  end

  def handle_info(:start, aSource) do
    aSource = %{ aSource | startReceived: aSource.startReceived+1 }
    if length(aSource.children)==aSource.startReceived do
        {:ok, listen_socket} = :gen_tcp.listen(aSource.port, [:binary, {:packet, 0}, {:active,true}, {:ip, aSource.ip}])
        {:ok, socket } = :gen_tcp.accept listen_socket
        { :noreply, %{ aSource | socket: socket, listen_socket: listen_socket } }
    else
      { :noreply, aSource }
    end
  end

end
