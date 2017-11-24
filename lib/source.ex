
defmodule Source do 
	@enforce_keys [ :id, :refreshRate, :fct, :state, :default ]
	defstruct [ :id, :refreshRate, :fct, :state, :default, :dependencies, 
	children: [], startReceived: 0, ref: nil ]

	use GenServer, restart: :transient

	# Client API

	def start_link(_opts, aSource) do
		{id, _node} = aSource.id
		GenServer.start_link(__MODULE__, aSource, name: id)
    end

	# Server API

	def init(%__MODULE__{}=aSource) do
		#IO.puts "init source #{inspect aSource.id}"
		Process.flag(:trap_exit, true)
		Enum.map(aSource.children, &DNode.propagateDefault(&1, aSource.id, aSource.default))
        { :ok, aSource }
    end

    def terminate(_reason, aSource) do
    	case aSource.ref do
    		nil -> nil
    		_ -> Process.cancel_timer(aSource.ref)
    	end
    end

    def handle_info(:produce, aSource) do
    	#IO.puts "produce #{inspect aSource.id}"
    	ref = Process.send_after(elem(aSource.id, 0), :produce, aSource.refreshRate)
    	{ value, newState } = aSource.fct.(aSource.state)
    	Enum.map(aSource.children, &DNode.propagate(&1, aSource.id, aSource.id, value))
    	{ :noreply, %{ aSource | state: newState, ref: ref } }
    end

    def handle_info(:start, aSource) do
    	#IO.puts "start #{inspect aSource.id}"
        aSource = %{ aSource | startReceived: aSource.startReceived+1 }
        if length(aSource.children)==aSource.startReceived do
            ref = Process.send_after(elem(aSource.id, 0), :produce, aSource.refreshRate)
            { :noreply, %{ aSource | ref: ref } }
        else
        	{ :noreply, aSource }
        end
    end

end