package lsm_tree

import (
	"testing"
)

func TestInsert(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	tree.Put(5, "value")
	tree.Put(6, "value")
	if tree.Size() != 2 {
		t.Errorf("Expected size to be 2, got %d", tree.Size())
	}
}

func TestMultipleInsert(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	elements := []struct {
		key   int
		value string
	}{
		{key: 3, value: "value3"},
		{key: 8, value: "value8"},
		{key: 2, value: "value2"},
		{key: 4, value: "value4"},
		{key: 6, value: "value5"},
	}
	for _, element := range elements {
		tree.Put(element.key, element.value)
	}

	if tree.Size() != 5 {
		t.Errorf("Expected size to be %d, got %d", len(elements), tree.Size())
	}
}

func TestDuplicates(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	tree.Put(5, "value")
	tree.Put(5, "value") // Inserting duplicate
	if tree.Size() != 2 {
		t.Error("Should be 2")
	}
}

func TestInsertAscendingOrder(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	elements := []struct {
		key   int
		value string
	}{
		{1, "value1"},
		{2, "value2"},
		{3, "value3"},
		{4, "value4"},
		{5, "value5"},
		{6, "value5"},
		{7, "value5"},
	}
	for _, element := range elements {
		tree.Put(element.key, element.value)
	}
	if tree.Size() != 7 {
		t.Error("Should be 7")
	}
}

func TestInsertDescendingOrder(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	elements := []struct {
		key   int
		value string
	}{
		{7, "value5"},
		{6, "value5"},
		{5, "value5"},
		{4, "value4"},
		{3, "value3"},
		{2, "value2"},
		{1, "value1"},
	}
	for _, element := range elements {
		tree.Put(element.key, element.value)
	}
	if tree.Size() != 7 {
		t.Error("Should be 7")
	}
}

func TestIteratorDescending(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	elements := []struct {
		key   int
		value string
	}{
		{5, "value5"},
		{4, "value4"},
		{3, "value3"},
		{2, "value2"},
		{1, "value1"},
	}
	for _, element := range elements {
		tree.Put(element.key, element.value)
	}
	if tree.Size() != 5 {
		t.Error("Should be 5")
	}

	iter := tree.Iterator()
	count := 0
	for iter.HasNext() {
		node := iter.Next()
		count++
		if node.Key != count {
			t.Errorf("%v should be %v", node.Key, count)
		}
	}
}

func TestIteratorAscending(t *testing.T) {
	tree := &RedBlackTree[int, any]{}
	elements := []struct {
		key   int
		value string
	}{
		{1, "value1"},
		{2, "value2"},
		{3, "value3"},
		{4, "value4"},
		{5, "value5"},
	}
	for _, element := range elements {
		tree.Put(element.key, element.value)
	}

	iter := tree.Iterator()
	count := 0
	for iter.HasNext() {
		node := iter.Next()
		count++
		if node.Key != count {
			t.Errorf("%v should be %v", node.Key, count)
		}
	}
}
