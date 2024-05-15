package testProtobuf

import "google.golang.org/protobuf/proto"

func (t *TestEntry) Apply(i interface{}) error {
	return nil
}

func (t *TestEntry) MarshalProto() proto.Message {
	return t
}
