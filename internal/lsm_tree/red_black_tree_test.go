package lsm_tree

import (
	"testing"
)

func TestRBT(t *testing.T) {
	t.Run("Test insert", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{}
		tree.Put(5, "value")
		tree.Put(6, "value")
		if tree.Size() != 2 {
			t.Errorf("Expected size to be 2, got %d", tree.Size())
		}
	})

	t.Run("Test multiple inserts", func(t *testing.T) {
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
	})

	t.Run("Test insert duplicates", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{}
		tree.Put(5, "value")
		tree.Put(5, "value") // Inserting duplicate
		if tree.Size() != 2 {
			t.Error("Should be 2")
		}
	})

	t.Run("Test insert keys in ascending order", func(t *testing.T) {
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
	})

	t.Run("Test insert keys in descending ordrer", func(t *testing.T) {
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
	})
}

func TestRBTIterator(t *testing.T) {
	t.Run("Descending order insert", func(t *testing.T) {
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
	})

	t.Run("Ascending order insert", func(t *testing.T) {
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
	})
}
