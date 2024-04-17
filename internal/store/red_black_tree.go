package store

import (
	"cmp"
	"fmt"
)

type Node[K cmp.Ordered, V any] struct {
	left, right, parent  *Node[K, V]
	Key                  K
	Value                V
	isLeftChild, isBlack bool
}

func (node *Node[K, V]) uncle() *Node[K, V] {
	if node == nil || node.parent == nil || node.parent.parent == nil {
		return nil
	}
	return node.parent.sibling()
}

func (node *Node[K, V]) sibling() *Node[K, V] {
	if node == nil || node.parent == nil {
		return nil
	}
	if node == node.parent.left {
		return node.parent.right
	}
	return node.parent.left
}

func (node *Node[K, V]) grandparent() *Node[K, V] {
	if node == nil || node.parent == nil || node.parent.parent == nil {
		return nil
	}
	return node.parent.parent
}

func newNode[K cmp.Ordered, V any](key K, val V) *Node[K, V] {
	return &Node[K, V]{isLeftChild: false, isBlack: false, Key: key, Value: val, left: nil, right: nil, parent: nil}
}

type RBTIterator[K cmp.Ordered, V any] struct {
	nodes []*Node[K, V]
	index int
}

func (self *RBTIterator[K, V]) HasNext() bool {
	return self.index < len(self.nodes)
}

func (self *RBTIterator[K, V]) Next() *Node[K, V] {
	if self.HasNext() {
		node := self.nodes[self.index]
		self.index++
		return node
	}
	return nil
}

func NewRBTIterator[K cmp.Ordered, V any](root *Node[K, V], size uint) *RBTIterator[K, V] {
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

type RedBlackTree[K cmp.Ordered, V any] struct {
	size uint
	root *Node[K, V]
}

func (self *RedBlackTree[K, V]) Iterator() Iterator[K, V] {
	return NewRBTIterator(self.root, self.Size())
}

func NewRedBlackTree() MemTable[int, any] {
	return &RedBlackTree[int, any]{}
}

func (self *RedBlackTree[K, V]) Size() uint {
	return self.size
}

// For debugging
func (self *RedBlackTree[K, V]) printTree(root *Node[K, V], space int) {
	if root != nil {
		space = space + 10
		self.printTree(root.right, space)
		fmt.Println("")
		for i := 10; i < space; i++ {
			fmt.Printf(" ")
		}
		fmt.Printf("%v", root.isBlack) // Print colors or keys
		fmt.Println("")
		self.printTree(root.left, space)
	}
}

func (self *RedBlackTree[K, V]) Put(key K, val V) {
	self.root = self.put(self.root, key, val)
	self.size++
	self.root.isBlack = true
}

func isRed[K cmp.Ordered, V any](node *Node[K, V]) bool {
	if node == nil {
		return false
	}
	return !node.isBlack
}

func (self *RedBlackTree[K, V]) put(node *Node[K, V], key K, val V) *Node[K, V] {
	if node == nil {
		return newNode(key, val)
	}
	comp := cmp.Compare(key, node.Key)
	if comp < 0 {
		node.left = self.put(node.left, key, val)
	} else if comp > 0 {
		node.right = self.put(node.right, key, val)
	} else {
		node.Value = val
	}

	if isRed(node.right) && !isRed(node.left) {
		node = self.leftRotate(node)
	}
	if isRed(node.left) && isRed(node.left.left) {
		node = self.rightRotate(node)
	}
	if isRed(node.left) && isRed(node.right) {
		self.colorFlip(node)
	}

	return node
}

func (self *RedBlackTree[K, V]) colorFlip(node *Node[K, V]) {
	node.isBlack = false
	node.left.isBlack = true
	node.right.isBlack = true
}

func (self *RedBlackTree[K, V]) rightRotate(node *Node[K, V]) *Node[K, V] {
	tmp := node.left
	node.left = tmp.right
	tmp.right = node
	tmp.isBlack = node.isBlack
	node.isBlack = true
	return tmp
}

func (self *RedBlackTree[K, V]) leftRotate(node *Node[K, V]) *Node[K, V] {
	tmp := node.right
	node.right = tmp.left
	tmp.left = node
	tmp.isBlack = node.isBlack
	node.isBlack = false
	return tmp
}
