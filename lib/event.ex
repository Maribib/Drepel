defmodule Event do
    @enforce_keys [ :id, :time, :name, :onRun, :eid ]
    defstruct [ :id, :time, :name, :onRun, :eid ]
end