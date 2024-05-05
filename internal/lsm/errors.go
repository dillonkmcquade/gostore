package lsm

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound will be returned when the key could not be located
	ErrNotFound = errors.New("not found")

	ErrInternal = fmt.Errorf("%w: internal error", ErrNotFound)
)
