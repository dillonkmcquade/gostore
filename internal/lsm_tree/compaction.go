package lsm_tree

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
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
//		- Oldest <-
//		- Tombstone density
//		- Tombstone-TTL

// type CompactionTask[K cmp.Ordered, V any] struct {
// 	upperLevel              *int             // if upper level is nil, is level0 compaction
// 	upperLevelIDs           []*SSTable[K, V] // filenames of SSTables in upper level
// 	lowerLevel              int
// 	lowerLevelIDs           []*SSTable[K, V] // filenames of SSTables in lower level
// 	isLowerLevelBottomLevel bool
// }

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
	fmt.Println("Compaction")
	allCompacted := true
	for _, level := range manifest.Levels {
		if c.Trigger(level) {
			allCompacted = false
			break
		}
	}

	if allCompacted {
		fmt.Println("Skipping compaction")
		return
	}

	for i, level := range manifest.Levels {
		if c.Trigger(level) {
			// level0 compaction -> Skip finding overlaps
			if level.Number == 0 {
				fmt.Println("Level 0 compaction")
				// Merge all tables
				merged := c.merge(level.Tables...)
				fmt.Printf("Merging %v tables\n", len(level.Tables))
				fmt.Printf("First: %v, Last %v, #entries: %v\n", merged.First, merged.Last, len(merged.Entries))

				// Split
				split := c.split(merged)

				fmt.Printf("Split tables: %v\n", len(split))

				// Write files and add to manifest
				for _, splitTable := range split {
					go func(tbl *SSTable[K, V], level *Level[K, V]) {
						tbl.Name = filepath.Join(c.LevelPaths[1], generateUniqueSegmentName(tbl.CreatedOn))

						fmt.Printf("Syncing to %v\n", tbl.Name)
						_, err := tbl.Sync()
						if err != nil {
							panic(err)
						}
						manifest.mut.Lock()
						manifest.Levels[level.Number+1].Add(tbl)
						manifest.mut.Unlock()
					}(splitTable, level)
				}

				for _, tbl := range level.Tables {
					go func(name string) { os.Remove(name) }(tbl.Name)
				}

				manifest.mut.Lock()
				manifest.Levels[0].Tables = []*SSTable[K, V]{}
				manifest.Levels[0].Size = 0
				manifest.mut.Unlock()
			} else {
				fmt.Println("Level 1+ compaction")
				// Choose oldest table
				table := findOldestTable(level.Tables)
				// find tables in lowerlevel that overlap with table in upper level
				overlaps := c.findOverlappingSSTables(table, manifest.Levels[i+1])

				if len(overlaps) == 0 {
					fmt.Println("Compacting into empty lower level")
					// move upperlevel -> lowerlevel
					newLocation := filepath.Join(c.LevelPaths[i+1], filepath.Base(table.Name))
					os.Rename(table.Name, newLocation)
					table.Name = newLocation

					// Update manifest
					manifest.mut.Lock()
					manifest.Levels[i+1].Add(table)
					manifest.Levels[i].Remove(table)
					manifest.mut.Unlock()
					return
				}

				merged := c.merge(append(overlaps, table)...)
				fmt.Printf("Merging %v tables\n", len(level.Tables))
				fmt.Printf("First: %v, Last %v, #entries: %v\n", merged.First, merged.Last, len(merged.Entries))

				// Split merged table into smaller sizes
				split := c.split(merged)
				fmt.Printf("Split tables: %v\n", len(split))

				manifest.mut.Lock()
				// Write files and add to manifest
				for _, splitTable := range split {
					splitTable.Name = filepath.Join(c.LevelPaths[i+1], fmt.Sprintf("%v.segment", splitTable.CreatedOn.Unix()))
					fmt.Printf("Syncing to %v\n", splitTable.Name)
					_, err := splitTable.Sync()
					if err != nil {
						panic(err)
					}
					manifest.Levels[i+1].Add(splitTable)
				}

				// Cleanup tables from lowerlevel
				for _, lap := range overlaps {
					manifest.Levels[i+1].Remove(lap)
				}
				manifest.mut.Unlock()

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
			if err != nil {
				fmt.Println(err)
				continue
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

// func (c *CompactionImpl[K, V]) generateCompactionTask(level int, manifest *Manifest[K, V]) []*CompactionTask[K, V] {
// 	tasks := []*CompactionTask[K, V]{}
// 	if level == 0 { // Compact all upperlevel tables
// 		task := &CompactionTask[K, V]{
// 			upperLevel:              nil,
// 			upperLevelIDs:           manifest.Levels[level].Tables,
// 			lowerLevel:              level + 1,
// 			lowerLevelIDs:           make([]*SSTable[K, V], 0),
// 			isLowerLevelBottomLevel: (level + 1) == 3,
// 		}
// 		for _, table := range manifest.Levels[level].Tables {
// 			for _, lowerLevelTable := range manifest.Levels[level+1].Tables {
// 				if table.Overlaps(lowerLevelTable) {
// 					task.lowerLevelIDs = append(task.lowerLevelIDs, lowerLevelTable)
// 				}
// 			}
// 		}
// 		return tasks
// 	}
//
// 	// Choose upperlevel tables that overlap lower  level tables
// 	task := &CompactionTask[K, V]{
// 		upperLevel:              &level,
// 		upperLevelIDs:           make([]*SSTable[K, V], 0),
// 		lowerLevel:              level + 1,
// 		lowerLevelIDs:           make([]*SSTable[K, V], 0),
// 		isLowerLevelBottomLevel: (level + 1) == 3,
// 	}
// 	for _, table := range manifest.Levels[level].Tables {
// 		for _, lowerLevelTable := range manifest.Levels[level+1].Tables {
// 			if table.Overlaps(lowerLevelTable) {
// 				task.lowerLevelIDs = append(task.lowerLevelIDs, lowerLevelTable)
// 				task.upperLevelIDs = append(task.upperLevelIDs, table)
// 			}
// 		}
// 	}
//
// 	return task
// }
