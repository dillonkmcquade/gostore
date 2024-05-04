package lsm_tree

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Creates directory if it does not exist.
func mkDir(filename string) error {
	path := filepath.Clean(filename)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0750)
	}
	return err
}

// Panics if statement does not resolve to true
func assert(stmt bool, msg string, args ...any) {
	if !stmt {
		panic(fmt.Sprintf(msg, args...))
	}
}

func remove[T any](slice []T, i int) []T {
	return append(slice[:i], slice[i+1:]...)
}

func insertAt[T any](slice []T, i int, val T) []T {
	if i >= len(slice) {
		return append(slice, val)
	}
	slice = append(slice[:i+1], slice[i:]...)
	slice[i] = val
	return slice
}

// Generate a random string of n bytes
func generateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
