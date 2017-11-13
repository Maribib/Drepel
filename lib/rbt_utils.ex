defmodule RedBlackTree.Utils do
    def first(%RedBlackTree{root: root}) do
        if (!is_nil(root)) do
            _first(root)
        else
            root
        end
    end

    def _first(%RedBlackTree.Node{}=aNode) do
        if (!is_nil(aNode.left)) do
            _first(aNode.left)
        else
            aNode.key
        end
    end

    def firstKV(%RedBlackTree{root: root}) do
        if is_nil(root) do
            root
        else
            _firstKV(root)
        end
    end

    def _firstKV(%RedBlackTree.Node{}=aNode) do
        if is_nil(aNode.left) do
            { aNode.key, aNode.value }
        else
            _firstKV(aNode.left)
        end
    end
end