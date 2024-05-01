package lsm_tree

import (
	"cmp"
	"fmt"
)

type RedBlackTree[K cmp.Ordered, V any] struct {
	size uint
	root *Node[K, V]
}

type Node[K cmp.Ordered, V any] struct {
	left, right, parent *Node[K, V]
	Key                 K
	Value               V
	Operation           Operation
	isBlack             bool
}

func (node *Node[K, V]) String() string {
	return fmt.Sprintf("Node(black: %v) {%v %v}", node.isBlack, node.Key, node.Value)
}

func newNode[K cmp.Ordered, V any](key K, val V, op Operation) *Node[K, V] {
	return &Node[K, V]{isBlack: false, Key: key, Value: val, left: nil, right: nil, parent: nil, Operation: op}
}

type RBTIterator[K cmp.Ordered, V any] struct {
	nodes []*Node[K, V]
	index int
}

func (iter *RBTIterator[K, V]) HasNext() bool {
	return iter.index < len(iter.nodes)
}

func (iter *RBTIterator[K, V]) Next() *Node[K, V] {
	if iter.HasNext() {
		node := iter.nodes[iter.index]
		iter.index++
		return node
	}
	return nil
}

func newRBTIterator[K cmp.Ordered, V any](root *Node[K, V], size uint) *RBTIterator[K, V] {
	list := make([]*Node[K, V], 0, size)
	sortedNodeList(root, &list)
	return &RBTIterator[K, V]{nodes: list}
}

// Traverses the tree inorder and appends each node to the list
func sortedNodeList[K cmp.Ordered, V any](root *Node[K, V], list *[]*Node[K, V]) {
	if root == nil {
		return
	}
	sortedNodeList(root.left, list)
	*list = append(*list, root)
	sortedNodeList(root.right, list)
}

func (rbt *RedBlackTree[K, V]) Iterator() Iterator[K, V] {
	return newRBTIterator(rbt.root, rbt.Size())
}

func (rbt *RedBlackTree[K, V]) Size() uint {
	return rbt.size
}

func (rbt *RedBlackTree[K, V]) Clear() {
	rbt.root = nil
	rbt.size = 0
}

// Insert or update value at key
func (rbt *RedBlackTree[K, V]) Put(key K, val V) {
	rbt.root = rbt.put(rbt.root, key, val, INSERT)
	rbt.root.isBlack = true
}

// Update node Op to be DELETE
func (rbt *RedBlackTree[K, V]) Delete(key K) {
	rbt.root = rbt.put(rbt.root, key, Node[K, V]{}.Value, DELETE)
	rbt.root.isBlack = true
}

func isRed[K cmp.Ordered, V any](node *Node[K, V]) bool {
	if node == nil {
		return false
	}
	return !node.isBlack
}

func (rbt *RedBlackTree[K, V]) put(node *Node[K, V], key K, val V, op Operation) *Node[K, V] {
	if node == nil {
		rbt.size++
		return newNode(key, val, op)
	}
	comp := cmp.Compare(key, node.Key)
	if comp < 0 {
		node.left = rbt.put(node.left, key, val, op)
	} else if comp > 0 {
		node.right = rbt.put(node.right, key, val, op)
	} else {
		node.Value = val
		node.Operation = op
	}

	if isRed(node.right) && !isRed(node.left) {
		node = rbt.leftRotate(node)
	}
	if isRed(node.left) && isRed(node.left.left) {
		node = rbt.rightRotate(node)
	}
	if isRed(node.left) && isRed(node.right) {
		rbt.colorFlip(node)
	}

	return node
}

func (rbt *RedBlackTree[K, V]) colorFlip(node *Node[K, V]) {
	node.isBlack = false
	node.left.isBlack = true
	node.right.isBlack = true
}

func (rbt *RedBlackTree[K, V]) rightRotate(node *Node[K, V]) *Node[K, V] {
	tmp := node.left
	node.left = tmp.right
	tmp.right = node
	tmp.isBlack = node.isBlack
	node.isBlack = true
	return tmp
}

func (rbt *RedBlackTree[K, V]) leftRotate(node *Node[K, V]) *Node[K, V] {
	tmp := node.right
	node.right = tmp.left
	tmp.left = node
	tmp.isBlack = node.isBlack
	node.isBlack = false
	return tmp
}

func (rbt *RedBlackTree[K, V]) Get(key K) (V, bool) {
	node := rbt.root

	return rbt.get(node, key)
}

func (rbt *RedBlackTree[K, V]) get(node *Node[K, V], key K) (V, bool) {
	if node == nil {
		return Node[K, V]{}.Value, false
	}
	if key > node.Key {
		return rbt.get(node.right, key)
	} else if key < node.Key {
		return rbt.get(node.left, key)
	} else {
		return node.Value, node.Operation != DELETE
	}
}
