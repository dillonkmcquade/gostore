package ordered

import (
	"cmp"
	"math/rand"
	"testing"
)

func TestRBT(t *testing.T) {
	t.Run("Test insert", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
		tree.Put(5, "value")
		tree.Put(6, "value")
		if tree.Size() != 2 {
			t.Errorf("Expected size to be 2, got %d", tree.Size())
		}
	})

	t.Run("Test multiple inserts", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
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
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
		tree.Put(5, "value")
		tree.Put(5, "value") // Inserting duplicate
		if tree.Size() != 1 {
			t.Error("Should be 1")
		}
	})

	t.Run("Test insert keys in ascending order", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
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
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
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

	t.Run("Test insert random numbers", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
		for i := 0; i < 1000; i++ {
			key := rand.Intn(10000)
			tree.Put(key, "TEST")
		}
	})
}

func TestRBTIterator(t *testing.T) {
	t.Run("Descending order insert", func(t *testing.T) {
		tree := &RedBlackTree[int, any]{comparator: cmp.Compare[int]}
		elements := []string{
			"value5",
			"value4",
			"value3",
			"value2",
			"value1",
		}
		for i, element := range elements {
			tree.Put(i, element)
		}
		if tree.Size() != 5 {
			t.Error("Should be 5")
		}

		iter := tree.Values()
		count := 0
		for iter.HasNext() {
			val := iter.Next()
			if val != elements[count] {
				t.Errorf("%v should be %v", val, elements[count])
			}
			count++
		}
	})

	t.Run("Ascending order insert", func(t *testing.T) {
		tree := &RedBlackTree[int, string]{comparator: cmp.Compare[int]}
		elements := []string{
			"value1",
			"value2",
			"value3",
			"value4",
			"value5",
		}
		for i, element := range elements {
			tree.Put(i, element)
		}

		iter := tree.Values()
		count := 0
		for iter.HasNext() {
			val := iter.Next()
			if val != elements[count] {
				t.Errorf("%v should be %v", val, elements[count])
			}
			count++
		}
	})
}
