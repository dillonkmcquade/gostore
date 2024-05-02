package lsm_tree

import (
	"cmp"
	"log/slog"
)

type FileIOType int

const (
	RENAME FileIOType = iota
	SYNC
	CREATE
	FREMOVE
	ENCODE
	DECODE
)

func (f FileIOType) String() string {
	choices := [...]string{"rename", "sync", "create", "remove", "encode", "decode"}
	if f < 0 || int(f) >= len(choices) {
		panic("Invalid type")
	}
	return choices[f]
}

type FileType int

const (
	DIR FileType = iota
	SSTABLE
	WALFILE
	MANIFEST
	BLOOMFILTER
)

func (f FileType) String() string {
	choices := [...]string{"DIR", "SSTable", "WAL", "manifest", "bloom"}
	if f < 0 || int(f) >= len(choices) {
		panic("Invalid type")
	}
	return choices[f]
}

func logFileIO[K cmp.Ordered, V any](t FileIOType, f FileType, args any) {
	switch f {
	case DIR:
		filename, ok := args.(string)
		if ok {
			slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "filename", filename)
		}
	case SSTABLE:
		table, ok := args.(*SSTable[K, V])
		if ok {
			slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "filename", table.Name)
		}
	case WALFILE:
		wal, ok := args.(string)
		if ok {
			slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "filename", wal)
		}
	case MANIFEST:
		man, ok := args.(*ManifestEntry[K, V])
		if ok {
			if man.Table == nil {
				slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "level", man.Level)
			} else {
				slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "level", man.Level, "table", man.Table.Name)
			}
		}
	case BLOOMFILTER:
		slog.Debug("FILE I/O", "Operation", t.String(), "filetype", f.String(), "name", args)
	}
}

func logError(err error) {
	slog.Error("error", "cause", err)
}
