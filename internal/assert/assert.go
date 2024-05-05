package assert

import (
	"cmp"
	"fmt"
	"reflect"
)

// Panics if statement does not resolve to true
func True(stmt bool, msg string, args ...any) {
	if !stmt {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Panics if statement does not resolve to false
func False(stmt bool, msg string, args ...any) {
	if stmt {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Panic if obj is not nil
func Nil(obj any, msg string, args ...any) {
	if obj != nil {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Panic if obj is nil
func NotNil(obj any, msg string, args ...any) {
	if obj == nil {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Panic if a is not less than b
func LessThan[T cmp.Ordered](a T, b T, msg string, args ...any) {
	if a > b {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Panic if a is not greater than b
func GreaterThan[T cmp.Ordered](a T, b T, msg string, args ...any) {
	if a < b {
		panic(fmt.Sprintf(msg, args...))
	}
}

// Equal panics if a and b are not "deeply equal"
func Equal(a any, b any, msg string, args ...any) {
	if !reflect.DeepEqual(a, b) {
		panic(fmt.Sprintf(msg, args...))
	}
}
