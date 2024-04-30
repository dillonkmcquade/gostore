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
	SSTable_max_size int
}

// Returns compaction task if level triggers a compaction
func (c *CompactionImpl[K, V]) Trigger(level *Level[K, V]) bool {
	return level.Size >= level.MaxSize
}

func generateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func generateUniqueSegmentName(time time.Time) string {
	uniqueString, err := generateRandomString(8)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%v_%v.segment", time.Unix(), uniqueString)
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

	for i, level := range manifest.Levels {
		if c.Trigger(level) {
			// level0 compaction -> Skip finding overlaps
			slog.Debug("Compaction", "level", level.Number)
			if level.Number == 0 {
				// Merge all tables
				merged := c.merge(level.Tables...)

				// Split
				split := c.split(merged)
				slog.Debug("split", "in", len(level.Tables), "out", len(split), "last table #entries", len(split[len(split)-1].Entries))

				var wg sync.WaitGroup
				// Write files and add to manifest
				for _, splitTable := range split {
					wg.Add(1)
					go func(tbl *SSTable[K, V]) {
						tbl.Name = filepath.Join(c.LevelPaths[1], generateUniqueSegmentName(tbl.CreatedOn))

						slog.Debug("Sync", "filename", tbl.Name)
						_, err := tbl.Sync()
						if err != nil {
							panic(err)
						}
						manifest.Levels[1].Add(tbl)
						wg.Done()
					}(splitTable)
				}

				for _, tbl := range level.Tables {
					wg.Add(1)
					go func(name string) {
						slog.Debug("Remove", "filename", name)
						os.Remove(name)
						wg.Done()
					}(tbl.Name)
				}
				wg.Wait()

				defer level.Clear()
			} else {
				// Choose oldest table
				table := findOldestTable(level.Tables)
				// find tables in lowerlevel that overlap with table in upper level
				overlaps := c.findOverlappingSSTables(table, manifest.Levels[i+1])

				if len(overlaps) == 0 {
					// move upperlevel -> lowerlevel
					newLocation := filepath.Join(c.LevelPaths[i+1], filepath.Base(table.Name))

					os.Rename(table.Name, newLocation)

					slog.Debug("File modification", "type", "rename", "old", table.Name, "new", newLocation)

					table.Name = newLocation

					// Update manifest
					manifest.Levels[i+1].Add(table)
					manifest.Levels[i].Remove(table)
					return
				}

				merged := c.merge(append(overlaps, table)...)

				// Split merged table into smaller sizes
				split := c.split(merged)

				// Write files and add to manifest
				for _, splitTable := range split {
					splitTable.Name = filepath.Join(c.LevelPaths[i+1], fmt.Sprintf("%v.segment", splitTable.CreatedOn.Unix()))
					slog.Debug("Sync", "level", level.Number+1, "filename", splitTable.Name)
					_, err := splitTable.Sync()
					if err != nil {
						panic(err)
					}
					manifest.Levels[i+1].Add(splitTable)
				}

				// Cleanup tables from lowerlevel
				for _, overlapping_table := range overlaps {
					manifest.Levels[i+1].Remove(overlapping_table)
				}

				// Cleanup table from upper level
				level.Remove(table)
			}
		}
	}
}

func findOldestTable[K cmp.Ordered, V any](tables []*SSTable[K, V]) *SSTable[K, V] {
	if len(tables) == 0 {
		return nil
	}

	oldest := tables[0]

	for i := 1; i < len(tables); i++ {
		if tables[i].CreatedOn.Before(oldest.CreatedOn) {
			oldest = tables[i]
		}
	}
	return oldest
}

func (c *CompactionImpl[K, V]) split(table *SSTable[K, V]) []*SSTable[K, V] {
	assert(len(table.Entries) > c.SSTable_max_size)

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
			Entries:   table.Entries[i : lastIndex+1],
			First:     table.Entries[i].Key,
			Last:      table.Entries[lastIndex].Key,
			CreatedOn: timestamp,
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
			err := table.Open() // We dont close because we arent keeping this table
			defer table.Close()
			if err != nil {
				panic(err)
			}
		}
		assert(len(table.Entries) > 0)

		for _, entry := range table.Entries {
			if entry.Operation == DELETE {
				tree.Delete(entry.Key)
			} else {
				tree.Put(entry.Key, entry.Value)
			}
		}
	}

	slog.Debug("Merge", "tree size", tree.size, "# tables", len(tables))

	sstable := &SSTable[K, V]{
		Entries: make([]*SSTableEntry[K, V], 0),
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
