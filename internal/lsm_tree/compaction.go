package lsm_tree

import (
	"cmp"
	"fmt"
	"path/filepath"
	"time"
)

// Compaction primitives:
//
// https://arxiv.org/pdf/2202.04522v2.pdf
//
// 1. Compaction Trigger - When to re-organize the data layout?
//		- Level saturations(size) <-
//		- # of sorted runs
//		- File staleness
//		- Space amplification
//		- Tombstone-TTL <- Implement later
// 2. Data Layout - How to layout data physically on storage?
//		- Tiering
//		- 1-leveling
//		- L-leveling
//		- Hybrid <-
// 3. Compaction Granularity - How much data to move at a time?
//		- Level
//		- Sorted runs
//		- File
//		- Multiple files <-
// 4. Data Movement Policy - Which block of data to be moved during reorganization?
//		- Round-robin
//		- Least overlapping parent
//		- Least overlapping grandparent
//		- Coldest
//		- Oldest
//		- Tombstone density
//		- Tombstone-TTL

type CompactionTask[K cmp.Ordered, V any] struct {
	upperLevel              *int             // if upper level is nil, is level0 compaction
	upperLevelIDs           []*SSTable[K, V] // filenames of SSTables in upper level
	lowerLevel              int
	lowerLevelIDs           []*SSTable[K, V] // filenames of SSTables in lower level
	isLowerLevelBottomLevel bool
}

type CompactionImpl[K cmp.Ordered, V any] struct{}

// TODO
func (c *CompactionImpl[K, V]) Compact(task *CompactionTask[K, V], manifest *Manifest[K, V]) error {
	// execute task, merging tables and splitting as needed
	for i, upperLevelTable := range task.upperLevelIDs {
		lowerLevelTable := task.lowerLevelIDs[i]

		// Load the two tables into memory
		_ = upperLevelTable.Open()
		defer upperLevelTable.Close()
		_ = lowerLevelTable.Open()
		defer lowerLevelTable.Close()

		// Merge upper table into lower table
		output := merge(upperLevelTable, lowerLevelTable)

		// Dynamically set name
		output.Name = filepath.Join(numberToPathMap[task.lowerLevel], fmt.Sprintf("%v.segment", output.CreatedOn.Unix()))

		// Save table to disk
		size, err := output.Sync()
		if err != nil {
			return err
		}
		// update manifest as we go
		manifest[task.lowerLevel].Add(output, size)

		lowerTableSize, err := lowerLevelTable.Size()
		if err != nil {
			return err
		}
		manifest[task.lowerLevel].Remove(lowerLevelTable, lowerTableSize)

		upperTableSize, err := upperLevelTable.Size()
		if err != nil {
			return err
		}
		manifest[*task.upperLevel].Remove(upperLevelTable, upperTableSize)

	}
	return nil
}

func (c *CompactionImpl[K, V]) findOverlappingSSTables(upper_table *SSTable[K, V], lower_level *Level[K, V]) []*SSTable[K, V] {
	overlaps := make([]*SSTable[K, V], 0)
	for _, lower_table := range lower_level.Tables {
		if upper_table.Overlaps(lower_table) {
			overlaps = append(overlaps, lower_table)
		}
	}
	return overlaps
}

func (c *CompactionImpl[K, V]) generateCompactionTask(level int, manifest *Manifest[K, V]) *CompactionTask[K, V] {
	if level == 0 { // Compact all upperlevel tables
		task := &CompactionTask[K, V]{
			upperLevel:              nil,
			upperLevelIDs:           manifest[level].Tables,
			lowerLevel:              level + 1,
			lowerLevelIDs:           make([]*SSTable[K, V], 0),
			isLowerLevelBottomLevel: (level + 1) == 3,
		}
		for _, table := range manifest[level].Tables {
			for _, lowerLevelTable := range manifest[level+1].Tables {
				if table.Overlaps(lowerLevelTable) {
					task.lowerLevelIDs = append(task.lowerLevelIDs, lowerLevelTable)
				}
			}
		}
		return task
	}

	// Choose upperlevel tables that overlap lower  level tables
	task := &CompactionTask[K, V]{
		upperLevel:              &level,
		upperLevelIDs:           make([]*SSTable[K, V], 0),
		lowerLevel:              level + 1,
		lowerLevelIDs:           make([]*SSTable[K, V], 0),
		isLowerLevelBottomLevel: (level + 1) == 3,
	}
	for _, table := range manifest[level].Tables {
		for _, lowerLevelTable := range manifest[level+1].Tables {
			if table.Overlaps(lowerLevelTable) {
				task.lowerLevelIDs = append(task.lowerLevelIDs, lowerLevelTable)
				task.upperLevelIDs = append(task.upperLevelIDs, table)
			}
		}
	}

	return task
}

// Returns compaction task if level triggers a compaction
func (c *CompactionImpl[K, V]) Trigger(level int, manifest *Manifest[K, V]) *CompactionTask[K, V] {
	if manifest[level].Size >= manifest[level].MaxSize {
		return c.generateCompactionTask(level, manifest)
	}
	return nil
}

// Merge creates a new SSTable from two sorted SSTables
func merge[K cmp.Ordered, V any](t1, old *SSTable[K, V]) *SSTable[K, V] {
	timestamp := time.Now()
	result := &SSTable[K, V]{
		Entries:   make([]*SSTableEntry[K, V], 0),
		CreatedOn: timestamp,
	}
	i, j := 0, 0
	for i < len(t1.Entries) && j < len(old.Entries) {
		entry1 := t1.Entries[i]
		entry2 := old.Entries[j]
		if entry1.Operation == DELETE {
			result.Entries = append(result.Entries, entry2)
			j++
			continue
		}
		if entry1.Key <= entry2.Key {
			result.Entries = append(result.Entries, entry1)
			i++
		} else {
			result.Entries = append(result.Entries, entry2)
			j++
		}
	}
	result.Entries = append(result.Entries, t1.Entries[i:]...)
	result.Entries = append(result.Entries, old.Entries[j:]...)
	result.First = result.Entries[0].Key
	result.Last = result.Entries[len(result.Entries)-1].Key
	return result
}
