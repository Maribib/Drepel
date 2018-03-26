require Logger

defmodule Balancer do
    defstruct [:clustNodes, :timer, :routing, :balancingInterval]

    use GenServer

    def computeMeanUtilByNode(reports) do
        Enum.reduce(reports, %{}, fn {node, {utilizations, _}}, acc ->
            meanUtil = Enum.reduce(utilizations, 0, fn {_, utilization}, acc ->
                acc + utilization
            end) / length(utilizations)
            Map.put(acc, node, meanUtil)
        end)
    end

    def computeTotalRunningTime({_, runningTimeById}) do
        Enum.reduce(runningTimeById, 0, fn {_, runningTime}, acc -> 
            acc + runningTime
        end)
    end

    def decideBalancing(utilById, maxMeanUtil, minMeanUtil, res) do
        obj = :math.pow(maxMeanUtil-minMeanUtil, 2)
        all = Enum.reduce(utilById, %{}, fn {id, util}, acc ->
            # :math.pow((maxMeanUtil-util)-(minMeanUtil+util), 2)
            Map.put(acc, id, :math.pow((maxMeanUtil-minMeanUtil-2*util), 2))
        end)
        {id, newObj} = Enum.min_by(all, &elem(&1, 1))
        if newObj<obj do
            remaining = Map.delete(utilById, id)
            util = utilById[id]
            decideBalancing(remaining, maxMeanUtil-util, minMeanUtil+util, res++[id])
        else
            res
        end
    end
    
    # Client API

    def start_link(_opts) do
       GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def reset(clustNodes, routing, balancingInterval) do
        GenServer.call(__MODULE__, {:reset, clustNodes, routing, balancingInterval})
    end

    def reset(node, clustNodes, routing, balancingInterval) do
        GenServer.call({__MODULE__, node}, {:reset, clustNodes, routing, balancingInterval})
    end

    def stop(node) do
        GenServer.call({__MODULE__, node}, :stop)
    end

    def stop do
        GenServer.call(__MODULE__, :stop)
    end

    def join(node, nodeName) do
        GenServer.call({__MODULE__, node}, {:join, nodeName})
    end

    # Server API

    def init(:ok) do
        { :ok, %__MODULE__{} }
    end

    def handle_call({:reset, clustNodes, routing, balancingInterval}, _from, state) do
        { :reply, :ok, %{ state | 
            clustNodes: clustNodes,
            timer: if balancingInterval>0 do
                Process.send_after(__MODULE__, :balance, balancingInterval)
            else
                nil
            end,
            routing: routing,
            balancingInterval: balancingInterval
        } }
    end

    def handle_call(:stop, _from, state) do
        Utils.cancelTimer(state.timer)
        { :reply, :ok, %{ state | timer: nil } }
    end

    def handle_call({:join, node}, _from, state) do
        { :reply, :ok, %{ state | clustNodes: state.clustNodes ++ [node] } }
    end

    def handle_info(:balance, state) do
        reports = Enum.reduce(state.clustNodes, %{}, &Map.put(&2, &1, Sampler.getReport(&1)))
        Logger.info(inspect reports)
        meanUtilByNode = computeMeanUtilByNode(reports)
        {{minNode, minMeanUtil}, {maxNode, maxMeanUtil}} = Enum.min_max_by(meanUtilByNode, &elem(&1, 1))
        totalRunnningTime = computeTotalRunningTime(reports[maxNode])
        {_, maxNodeRunningTime} = reports[maxNode]
        utilById = Enum.reduce(maxNodeRunningTime, %{}, fn {id, runningTime}, acc -> 
            Map.put(acc, id, runningTime*maxMeanUtil/totalRunnningTime)
        end)
        ids = decideBalancing(utilById, maxMeanUtil, minMeanUtil, [])
        Logger.info("Balance decision: move #{inspect ids} from #{maxNode} to #{minNode}")
        if length(ids)>0 do
            Checkpoint.stop()
            cnt = Enum.map(ids, &GenServer.call({&1, state.routing[&1]}, {:addRepNode, minNode}))
            |> Enum.filter(&(&1==:ok))
            |> Enum.count()
            if cnt>0 do
                Checkpoint.injectAndWaitForCompletion([maxNode, minNode, ids])
            else
                Drepel.Env.move(minNode, ids)
            end
            { :noreply, state }
        else
            { :noreply, %{ state |
                timer: Process.send_after(__MODULE__, :balance, state.balancingInterval),
            } }
        end
    end
    
end