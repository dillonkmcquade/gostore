package wal

import (
	"encoding/binary"
	"io"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type BatchWriter struct {
	err    error
	writer io.Writer
}

func NewBatchWriter(writer io.Writer) *BatchWriter {
	return &BatchWriter{writer: writer}
}

func (w *BatchWriter) Write(e LogEntry) {
	if w.err != nil {
		return
	}

	if isZeroValue(e) {
		return
	}

	b, err := proto.Marshal(e.MarshalProto())
	if err != nil {
		w.err = err
		return
	}

	messageLength := uint64(len(b))

	buf := make([]byte, 0, len(b)+8)

	buf = binary.LittleEndian.AppendUint64(buf, messageLength)
	buf = append(buf, b...)

	_, err = w.writer.Write(buf)
	if err != nil {
		w.err = err
		return
	}
}

func (w *BatchWriter) Err() error {
	return w.err
}

func isZeroValue(i interface{}) bool {
	v := reflect.ValueOf(i)
	if !v.IsValid() {
		return true // Nil interface value
	}
	zero := reflect.Zero(v.Type()).Interface()
	return reflect.DeepEqual(i, zero)
}
