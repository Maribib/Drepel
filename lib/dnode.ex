defmodule DNode do
    @enforce_keys [:id]
    defstruct [ :id, parents: [], children: [], runFct: &Drepel.doNothing/1 ]
end