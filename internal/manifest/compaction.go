package manifest

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/sstable"
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

func (man *Manifest) Compact() {
	for {
		select {
		case <-man.done:
			return
		case <-man.compactionTicker.C:
			allCompacted := true
			for _, level := range man.Levels {
				if man.Trigger(level) {
					slog.Debug("Compaction triggered")
					allCompacted = false
					break
				}
			}

			if allCompacted {
				slog.Debug("Skipping compaction")
				continue
			}

			for _, level := range man.Levels {
				if man.Trigger(level) {
					man.mut.Lock()
					if level.Number == 0 {
						man.level_0_compact(level)
					} else {
						man.lower_level_compact(level)
					}
					man.mut.Unlock()
				}
			}
		}
	}
}

// Returns compaction task if level triggers a compaction
func (m *Manifest) Trigger(level *Level) bool {
	m.mut.Lock()
	defer m.mut.Unlock()
	return level.Size >= level.MaxSize
}

// The goal of L0 compaction is to insert the unsorted collection of sorted tables into the sorted L1.
//
// All tables from L0 are merged->split->sync->L1
func (man *Manifest) level_0_compact(level *Level) {
	man.waitForCompaction.Add(1)
	slog.Debug("============ Level 0 Compaction =============")
	// Merge all tables
	merged := sstable.Merge(level.Tables...)

	// Split
	split := sstable.Split(merged, man.SSTable_max_size, &sstable.Opts{
		BloomOpts: &filter.Opts{
			Size: uint64(man.SSTable_max_size * 10),
			Path: man.BloomPath,
		},
	})

	// Write files and add to manifest
	for _, splitTable := range split {
		splitTable.Name = filepath.Join(man.Levels[1].Path, sstable.GenerateUniqueSegmentName(splitTable.CreatedOn))

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

		man.Levels[1].Add(splitTable)
		entry := &ManifestEntry{Op: ADDTABLE, Table: splitTable, Level: 1}
		err = man.wal.Write(entry)
		if err != nil {
			slog.Error("Failed to add table to level 1", "filename", splitTable.Name)
			panic(err)
		}
	}

	var wg sync.WaitGroup
	for _, tbl := range level.Tables {
		wg.Add(1)
		go func(t *sstable.SSTable) {
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

	man.Levels[0].Clear()
	entry := &ManifestEntry{Op: CLEARTABLE, Table: nil, Level: 0}
	err := man.wal.Write(entry)
	if err != nil {
		slog.Error("Failed to clear level")
		panic(err)
	}
	man.waitForCompaction.Done()
}

// Merge oldest table from upper level into overlapping lower level tables
func (man *Manifest) lower_level_compact(level *Level) {
	// Choose oldest table
	table := sstable.Oldest(level.Tables)
	// find tables in lowerlevel that overlap with table in upper level
	overlaps := sstable.Overlapping(table, man.Levels[level.Number+1].Tables)

	// if lower level is empty, simply move the table from upper level to lower level
	if len(overlaps) == 0 {
		newLocation := filepath.Join(man.Levels[level.Number+1].Path, filepath.Base(table.Name))

		os.Rename(table.Name, newLocation)

		table.Name = newLocation

		// Update manifest
		man.AddTable(table, level.Number+1)
		man.RemoveTable(table, level.Number)
		return
	}

	merged := sstable.Merge(append(overlaps, table)...)

	// Split merged table into smaller sizes
	split := sstable.Split(merged, man.SSTable_max_size, &sstable.Opts{
		BloomOpts: &filter.Opts{
			Size: uint64(man.SSTable_max_size * 10),
			Path: man.BloomPath,
		},
	})

	// Write files and add to manifest
	for _, splitTable := range split {
		splitTable.Name = filepath.Join(man.Levels[level.Number+1].Path, fmt.Sprintf("%v.segment", splitTable.CreatedOn.Unix()))

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
		err = man.AddTable(splitTable, level.Number+1)
		if err != nil {
			slog.Error("Failed to add table to level 1", "filename", splitTable.Name)
			panic(err)
		}
	}

	// Cleanup tables from lowerlevel
	for _, overlapping_table := range overlaps {
		err := man.RemoveTable(overlapping_table, level.Number+1)
		if err != nil {
			slog.Error("Failure to remove table", "filename", overlapping_table.Name)
			panic(err)
		}
	}

	// Cleanup table from upper level
	err := man.RemoveTable(table, level.Number)
	if err != nil {
		slog.Error("Failure to remove table", "filename", table.Name)
		panic(err)
	}
	man.waitForCompaction.Done()
}
