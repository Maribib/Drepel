defmodule Distrib do
    def inc(x) do
        x+1
    end

    def count(_x, acc) do
    	acc+1
    end

    def greaterThan10(x) do
    	x>10
    end
end