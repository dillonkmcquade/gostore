package manifest

import (
	"cmp"
	"sort"

	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/sstable"
)

type Level[K cmp.Ordered, V any] struct {
	Tables  []*sstable.SSTable[K, V]
	Path    string
	Number  int
	Size    int64
	MaxSize int64
	// mut     sync.RWMutex
}

// Binary search the current level for table that has range overlapping key
func (l *Level[K, V]) BinarySearch(key K) (int, bool) {
	// l.mut.RLock()
	// defer l.mut.RUnlock()
	low := 0
	high := len(l.Tables) - 1

	for low <= high {
		mid := low + (high-low)/2
		if l.Tables[mid].First <= key && key <= l.Tables[mid].Last {
			return mid, true
		}

		if key < l.Tables[mid].First {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1, false
}

func (l *Level[K, V]) Add(table *sstable.SSTable[K, V]) {
	// l.mut.Lock()
	// defer l.mut.Unlock()
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += table.Size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First >= table.First && l.Tables[i].Last >= table.Last })
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += table.Size
}

func (l *Level[K, V]) Remove(table *sstable.SSTable[K, V]) {
	assert.True(len(l.Tables) > 0, "Expected table len > 0, found %v", len(l.Tables))
	// l.mut.Lock()
	// defer l.mut.Unlock()

	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First >= table.First && l.Tables[i].Last >= table.Last })
	assert.True(index < len(l.Tables), "Index %v out of range %v", index, len(l.Tables))
	if index < len(l.Tables) && l.Tables[index] == table {
		l.Tables = remove(l.Tables, index)
		l.Size -= table.Size
	}
}

// Assigns an empty array to Tables and sets size to 0
func (l *Level[K, V]) Clear() {
	// l.mut.Lock()
	// defer l.mut.Unlock()
	l.Tables = []*sstable.SSTable[K, V]{}
	l.Size = 0
}
