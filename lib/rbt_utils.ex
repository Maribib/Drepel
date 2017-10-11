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
end