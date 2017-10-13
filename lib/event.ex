defmodule Event do
    @enforce_keys [ :id, :time, :toRun, :onRun ]
    defstruct [ :id, :time, :toRun, :onRun ]
end