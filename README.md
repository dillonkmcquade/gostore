TODO:

- [x] Red-Black tree implementation
- [x] WAL
- [x] Bloom filter
- [x] Flush memtable to disk
- [x] Compaction
- [x] Logging
- [x] Config file
- [x] Manifest file for recreating manifest state
- [x] Make bloomfilter crash tolerant(move bloom filter to each sstable, switch to bitwise operations)
- [x] Logging
- [x] Test concurrent ops
- [] TCP serialization protocol to remove GRPC dependency

Prio:

- [] Improve read speed
  -[] Create key-offset map index on each sstable
  -[] Use mmap for each sstable to allow quick randomized access
- [] Test deletes + reading from compacted tree
- [] Test level 1+ compaction
