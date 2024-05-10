package pb

import "github.com/dillonkmcquade/gostore/internal/ordered"

func (e *SSTable_Entry) Apply(c interface{}) {
	rbt := c.(*ordered.RedBlackTree[[]byte, *SSTable_Entry])
	if e.Op == Operation_INSERT {
		rbt.Put(e.Key, e)
	}
}
