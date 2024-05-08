package assert

import (
	"reflect"
	"testing"
)

func TestTrue(t *testing.T) {
	tests := []struct {
		stmt bool
		msg  string
		args []interface{}
	}{
		{true, "This should not panic", nil},
		{false, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != !tt.stmt {
					t.Errorf("True(%v, %q, %v) panicked unexpectedly", tt.stmt, tt.msg, tt.args)
				}
			}()

			True(tt.stmt, tt.msg, tt.args...)
		}()
	}
}

func TestFalse(t *testing.T) {
	tests := []struct {
		stmt bool
		msg  string
		args []interface{}
	}{
		{false, "This should not panic", nil},
		{true, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != tt.stmt {
					t.Errorf("False(%v, %q, %v) panicked unexpectedly", tt.stmt, tt.msg, tt.args)
				}
			}()

			False(tt.stmt, tt.msg, tt.args...)
		}()
	}
}

func TestNil(t *testing.T) {
	tests := []struct {
		obj  interface{}
		msg  string
		args []interface{}
	}{
		{nil, "This should not panic", nil},
		{"not nil", "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != (tt.obj != nil) {
					t.Errorf("Nil(%v, %q, %v) panicked unexpectedly", tt.obj, tt.msg, tt.args)
				}
			}()

			Nil(tt.obj, tt.msg, tt.args...)
		}()
	}
}

func TestNotNil(t *testing.T) {
	tests := []struct {
		obj  interface{}
		msg  string
		args []interface{}
	}{
		{"not nil", "This should not panic", nil},
		{nil, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != (tt.obj == nil) {
					t.Errorf("NotNil(%v, %q, %v) panicked unexpectedly", tt.obj, tt.msg, tt.args)
				}
			}()

			NotNil(tt.obj, tt.msg, tt.args...)
		}()
	}
}

func TestLessThan(t *testing.T) {
	tests := []struct {
		a, b interface{}
		msg  string
		args []interface{}
	}{
		{1, 2, "This should not panic", nil},
		{2, 1, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != (tt.a.(int) > tt.b.(int)) {
					t.Errorf("LessThan(%v, %v, %q, %v) panicked unexpectedly", tt.a, tt.b, tt.msg, tt.args)
				}
			}()

			LessThan(tt.a.(int), tt.b.(int), tt.msg, tt.args...)
		}()
	}
}

func TestGreaterThan(t *testing.T) {
	tests := []struct {
		a, b interface{}
		msg  string
		args []interface{}
	}{
		{2, 1, "This should not panic", nil},
		{1, 2, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != (tt.a.(int) < tt.b.(int)) {
					t.Errorf("GreaterThan(%v, %v, %q, %v) panicked unexpectedly", tt.a, tt.b, tt.msg, tt.args)
				}
			}()

			GreaterThan(tt.a.(int), tt.b.(int), tt.msg, tt.args...)
		}()
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		a, b interface{}
		msg  string
		args []interface{}
	}{
		{1, 1, "This should not panic", nil},
		{1, 2, "This should panic", nil},
	}

	for _, tt := range tests {
		func() {
			defer func() {
				if r := recover(); (r != nil) != !reflect.DeepEqual(tt.a, tt.b) {
					t.Errorf("Equal(%v, %v, %q, %v) panicked unexpectedly", tt.a, tt.b, tt.msg, tt.args)
				}
			}()

			Equal(tt.a, tt.b, tt.msg, tt.args...)
		}()
	}
}
