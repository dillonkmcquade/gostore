package pb

import (
	"github.com/dillonkmcquade/gostore/internal/ordered"
	"google.golang.org/protobuf/proto"
)

func (e *SSTable_Entry) Apply(c interface{}) error {
	rbt := c.(*ordered.RedBlackTree[[]byte, *SSTable_Entry])
	if e.Op == Operation_OPERATION_INSERT {
		rbt.Put(e.Key, e)
	}
	return nil
}

func (e *SSTable_Entry) MarshalProto() proto.Message {
	return e
}
