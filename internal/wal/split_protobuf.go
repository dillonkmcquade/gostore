package wal

import (
	"encoding/binary"
)

func SplitProtobuf(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 && atEOF {
		return 0, nil, nil
	}
	if len(data) < 8 {
		return 0, nil, nil
	}
	lengthPrefix := binary.LittleEndian.Uint64(data[:8])
	totalLength := int(lengthPrefix) + 8
	if len(data) < totalLength {
		return 0, nil, nil
	}
	return totalLength, data[8:totalLength], nil
}
