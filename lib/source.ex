defmodule Source do
    use Task

    def start_link(_opts, fct) do
        Task.start_link(fct)
    end
end