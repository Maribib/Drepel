defmodule Sentinel do
    defstruct []
end

defmodule Message do
    @enforce_keys [:source, :sender, :value, :timestamp, :chckptId]
    defstruct [:source, :sender, :value, :timestamp, :chckptId]
end

defmodule ChckptMessage do
	@enforce_keys [:sender, :id]
    defstruct [:sender, :id]
end

defmodule MockNode do
    @enforce_keys [:id]
    defstruct [:id]
end