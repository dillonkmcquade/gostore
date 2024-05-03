package lsm_tree

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
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
//		- Oldest <-
//		- Tombstone density
//		- Tombstone-TTL

type CompactionImpl[K cmp.Ordered, V any] struct {
	LevelPaths       []string
	BloomPath        string
	SSTable_max_size int
}

// Returns compaction task if level triggers a compaction
func (c *CompactionImpl[K, V]) Trigger(level *Level[K, V]) bool {
	return level.Size >= level.MaxSize
}

// Generate a random string of n bytes
func generateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// Generate a unique SSTable filename in the format TIMESTAMP_UNIQUESTRING.segment
func generateUniqueSegmentName(time time.Time) string {
	uniqueString, err := generateRandomString(8)
	if err != nil {
		slog.Error("error occurred while generating random string")
		panic(err)
	}
	return fmt.Sprintf("%v_%v.segment", time.Unix(), uniqueString)
}

// The goal of L0 compaction is to insert the unsorted collection of sorted tables into the sorted L1.
//
// All tables from L0 are merged->split->sync->L1
func (c *CompactionImpl[K, V]) level_0_compact(level *Level[K, V], manifest *Manifest[K, V]) {
	// Merge all tables
	merged := c.merge(level.Tables...)

	// Split
	split := c.split(merged)

	var wg sync.WaitGroup
	// Write files and add to manifest
	for _, splitTable := range split {
		wg.Add(1)
		go func(tbl *SSTable[K, V]) {
			tbl.Name = filepath.Join(c.LevelPaths[1], generateUniqueSegmentName(tbl.CreatedOn))

			_, err := tbl.Sync()
			if err != nil {
				slog.Error("Failed to sync table", "filename", tbl.Name)
				panic(err)
			}

			err = tbl.SaveFilter()
			if err != nil {
				slog.Error("Failed to save filter", "filename", tbl.Filter.Name)
				panic(err)
			}

			err = manifest.AddTable(tbl, 1)
			if err != nil {
				slog.Error("Failed to add table to level 1", "filename", tbl.Name)
				panic(err)
			}
			wg.Done()
		}(splitTable)
	}

	for _, tbl := range level.Tables {
		wg.Add(1)
		go func(t *SSTable[K, V]) {
			err := os.Remove(t.Name)
			if err != nil {
				slog.Warn("Failure to remove table", "filename", t.Name)
			}
			err = os.Remove(t.Filter.Name)
			if err != nil {
				slog.Warn("Failure to remove filter", "filename", t.Filter.Name)
			}
			wg.Done()
		}(tbl)
	}
	wg.Wait()

	err := manifest.ClearLevel(level.Number)
	if err != nil {
		slog.Error("Failed to clear level")
		panic(err)
	}
}

// Merge oldest table from upper level into overlapping lower level tables
func (c *CompactionImpl[K, V]) lower_level_compact(level *Level[K, V], manifest *Manifest[K, V]) {
	// Choose oldest table
	table := findOldestTable(level.Tables)
	// find tables in lowerlevel that overlap with table in upper level
	overlaps := c.findOverlappingSSTables(table, manifest.Levels[level.Number+1])

	// if lower level is empty, simply move the table from upper level to lower level
	if len(overlaps) == 0 {
		newLocation := filepath.Join(c.LevelPaths[level.Number+1], filepath.Base(table.Name))

		os.Rename(table.Name, newLocation)

		table.Name = newLocation

		// Update manifest
		manifest.AddTable(table, level.Number+1)
		manifest.RemoveTable(table, level.Number)
		return
	}

	merged := c.merge(append(overlaps, table)...)

	// Split merged table into smaller sizes
	split := c.split(merged)

	// Write files and add to manifest
	for _, splitTable := range split {
		splitTable.Name = filepath.Join(c.LevelPaths[level.Number+1], fmt.Sprintf("%v.segment", splitTable.CreatedOn.Unix()))

		_, err := splitTable.Sync()
		if err != nil {
			slog.Error("Failed to sync table", "filename", splitTable.Name)
			panic(err)
		}
		err = splitTable.SaveFilter()
		if err != nil {
			slog.Error("Failed to save filter", "filename", splitTable.Filter.Name)
			panic(err)
		}
		err = manifest.AddTable(splitTable, level.Number+1)
		if err != nil {
			slog.Error("Failed to add table to level 1", "filename", splitTable.Name)
			panic(err)
		}
	}

	// Cleanup tables from lowerlevel
	for _, overlapping_table := range overlaps {
		err := manifest.RemoveTable(overlapping_table, level.Number+1)
		if err != nil {
			slog.Error("Failure to remove table", "filename", overlapping_table.Name)
			panic(err)
		}
	}

	// Cleanup table from upper level
	err := manifest.RemoveTable(table, level.Number)
	if err != nil {
		slog.Error("Failure to remove table", "filename", table.Name)
		panic(err)
	}
}

func (c *CompactionImpl[K, V]) Compact(manifest *Manifest[K, V]) {
	allCompacted := true
	for _, level := range manifest.Levels {
		if c.Trigger(level) {
			allCompacted = false
			break
		}
	}

	if allCompacted {
		return
	}

	for _, level := range manifest.Levels {
		if c.Trigger(level) {
			if level.Number == 0 {
				c.level_0_compact(level, manifest)
			} else {
				c.lower_level_compact(level, manifest)
			}
		}
	}
}

func findOldestTable[K cmp.Ordered, V any](tables []*SSTable[K, V]) *SSTable[K, V] {
	assert(len(tables) > 0, "Cannot find oldest table from slice of length 0")

	oldest := tables[0]

	for i := 1; i < len(tables); i++ {
		if tables[i].CreatedOn.Before(oldest.CreatedOn) {
			oldest = tables[i]
		}
	}
	return oldest
}

func (c *CompactionImpl[K, V]) split(table *SSTable[K, V]) []*SSTable[K, V] {
	assert(len(table.Entries) > c.SSTable_max_size, "Table too small to split")

	var tables []*SSTable[K, V]
	offset := c.SSTable_max_size

	var i int
	for i = 0; i < len(table.Entries); {
		var lastIndex int
		if i+int(offset)-1 < 0 {
			lastIndex = 0
		} else {
			lastIndex = min(i+int(offset)-1, len(table.Entries)-1)
		}

		timestamp := time.Now()
		tbl := &SSTable[K, V]{
			Entries: table.Entries[i : lastIndex+1],
			First:   table.Entries[i].Key,
			Last:    table.Entries[lastIndex].Key,
			Filter: NewBloomFilter[K](&BloomFilterOpts{
				Size: uint64(offset * 10),
				Path: c.BloomPath,
			}),
			CreatedOn: timestamp,
		}
		for _, e := range table.Entries[i : lastIndex+1] {
			tbl.Filter.Add(e.Key)
		}
		tables = append(tables, tbl)

		i += int(offset)

	}
	return tables
}

// Merge creates a new SSTable from multiple sorted SSTables
func (c *CompactionImpl[K, V]) merge(tables ...*SSTable[K, V]) *SSTable[K, V] {
	tree := &RedBlackTree[K, V]{}

	for _, table := range tables {
		if len(table.Entries) == 0 {
			err := table.Open()
			defer table.Close()
			if err != nil {
				slog.Error("merge: error opening table", "filename", table.Name)
				panic(err)
			}
		}
		assert(len(table.Entries) > 0, "Expected table with entries, found %v entries", len(table.Entries))

		for _, entry := range table.Entries {
			if entry.Operation == DELETE {
				tree.Delete(entry.Key)
			} else {
				tree.Put(entry.Key, entry.Value)
			}
		}
	}

	sstable := &SSTable[K, V]{
		Entries: make([]*SSTableEntry[K, V], 0, tree.Size()),
	}

	iter := tree.Iterator()
	for iter.HasNext() {
		node := iter.Next()
		entry := &SSTableEntry[K, V]{Key: node.Key, Value: node.Value, Operation: node.Operation}
		sstable.Entries = append(sstable.Entries, entry)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
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
