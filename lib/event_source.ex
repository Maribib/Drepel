require Source

defmodule EventSource do
  @enforce_keys [ :id, :port, :default ]
  defstruct [ :id, :port, :default, :dependencies, :socket, 
  :listen_socket, :chckptInterval, :chckptTimer,
  children: [], startReceived: 0, ip: {127,0,0,1}, repNodes: [], 
  chckptId: 0, routing: %{}]
  
  use GenServer, restart: :transient

  # Client API

  def start_link(_opts, aSource) do 
    GenServer.start_link(__MODULE__, aSource, name: aSource.id)
  end

  def init(%__MODULE__{id: id}=aSource) do
    #IO.puts "init event-source #{inspect aSource.id}"
    Process.flag(:trap_exit, true)
    chckptTimer = if aSource.chckptInterval>0 do
      Process.send_after(id, :checkpoint, aSource.chckptInterval)
    else
      nil
    end
    Enum.map(aSource.children, &Signal.propagateDefault(Map.get(aSource.routing, &1), &1, id, aSource.default))
    {:ok, %{ aSource | chckptTimer: chckptTimer } }
  end

  def terminate(_reason, aSource) do
      :gen_tcp.close(aSource.socket)
      :gen_tcp.close(aSource.listen_socket)
      Utils.cancelTimer(aSource.chckptTimer)
  end

  # Server API

  def handle_info({:tcp, _socket, value}, aSource) do
    message = %Message{
      source: aSource.id,
      sender: aSource.id,
      value: value,
      chckptId: aSource.chckptId
    }
    Enum.map(aSource.repNodes, &Store.put(&1, aSource.chckptId, message))
    Enum.map(aSource.children, &Signal.propagate(Map.get(aSource.routing, &1), &1, message))
    {:noreply, aSource}
  end

  def handle_info(:checkpoint, aSource) do
      chckptTimer = Process.send_after(aSource.id, :checkpoint, aSource.chckptInterval)
      message = %ChckptMessage{ 
          id: aSource.chckptId,
          sender: aSource.id 
      }
      Enum.map(aSource.children, &Signal.propagateChckpt(Map.get(aSource.routing, &1), &1, message))
      { :noreply, %{ aSource | 
          chckptTimer: chckptTimer, 
          chckptId: aSource.chckptId+1
      } }
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
