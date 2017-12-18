defmodule Distrib do
    def inc(x) do
        x+1
    end

    def count(_x, acc) do
    	res = acc+1
    	{ res, res }
    end

    def greaterThan10(x) do
    	x>10
    end
end