
defmodule Source do 
	@enforce_keys [ :id, :refreshRate, :fct, :state, :default ]
	defstruct [ :id, :refreshRate, :fct, :state, :default, :dependencies, 
	children: [], startReceived: 0 ]

	use GenServer, restart: :transient

	# Client API

	def start_link(_opts, aSource) do
		{id, _node} = aSource.id
		GenServer.start_link(__MODULE__, aSource, name: id)
    end

	# Server API

	def init(%__MODULE__{}=aSource) do
		#IO.puts "init source #{inspect aSource.id}"
		Enum.map(aSource.children, &DNode.propagateDefault(&1, aSource.id, aSource.default))
        { :ok, aSource }
    end

    def handle_info(:produce, aSource) do
    	#IO.puts "produce #{inspect aSource.id}"
    	Process.send_after(elem(aSource.id, 0), :produce, aSource.refreshRate)
    	{ value, newState } = aSource.fct.(aSource.state)
    	Enum.map(aSource.children, &DNode.propagate(&1, aSource.id, aSource.id, value))
    	{ :noreply, %{ aSource | state: newState } }
    end

    def handle_info(:start, aSource) do
    	#IO.puts "start #{inspect aSource.id}"
        aSource = %{ aSource | startReceived: aSource.startReceived+1 }
        if length(aSource.children)==aSource.startReceived do
            Process.send_after(elem(aSource.id, 0), :produce, aSource.refreshRate)
        end
        { :noreply, aSource }
    end

end