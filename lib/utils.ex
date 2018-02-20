defmodule Utils do
	def cancelTimer(nil) do
        nil
    end

    def cancelTimer(timer) do
        Process.cancel_timer(timer)
    end
end