package manifest

import (
	"slices"
	"sort"

	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/sstable"
)

type Level struct {
	Tables  []*sstable.SSTable
	Path    string
	Number  int
	Size    int64
	MaxSize int64
}

// Binary search the current level for table that has range overlapping key
func (l *Level) BinarySearch(key []byte) (int, bool) {
	low := 0
	high := len(l.Tables) - 1

	for low <= high {
		mid := low + (high-low)/2
		if slices.Compare(l.Tables[mid].First, key) <= 0 && slices.Compare(l.Tables[mid].Last, key) >= 0 {
			return mid, true
		}

		if slices.Compare(key, l.Tables[mid].First) < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1, false
}

func (l *Level) Add(table *sstable.SSTable) {
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += table.Size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool {
		case1 := slices.Compare(l.Tables[i].First, table.First) >= 0
		case2 := slices.Compare(l.Tables[i].Last, table.Last) >= 0
		return case1 && case2
	})
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += table.Size
}

func (l *Level) Remove(table *sstable.SSTable) {
	assert.True(len(l.Tables) > 0, "Expected table len > 0, found %v", len(l.Tables))

	index := sort.Search(len(l.Tables), func(i int) bool {
		case1 := slices.Compare(l.Tables[i].First, table.First) >= 0
		case2 := slices.Compare(l.Tables[i].Last, table.Last) >= 0
		return case1 && case2
	})
	assert.True(index < len(l.Tables), "Index %v out of range %v", index, len(l.Tables))
	if index < len(l.Tables) && l.Tables[index] == table {
		l.Tables = remove(l.Tables, index)
		l.Size -= table.Size
	}
}

// Assigns an empty array to Tables and sets size to 0
func (l *Level) Clear() {
	l.Tables = []*sstable.SSTable{}
	l.Size = 0
}
