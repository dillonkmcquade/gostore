package internal

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Generate a random string of n bytes
func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("rand.Read: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}
