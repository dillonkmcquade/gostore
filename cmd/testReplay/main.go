package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/dillonkmcquade/gostore/internal/lsm"
)

var logLevel = new(slog.LevelVar)

func main() {
	logLevel.Set(slog.LevelDebug)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	userHome := os.Getenv("HOME")
	gostoreHome := filepath.Join(userHome, ".gostore")

	opts := lsm.NewTestLSMOpts(gostoreHome)
	tree, err := lsm.New(opts)
	if err != nil {
		fmt.Printf("%v", err)
		panic(err)
	}

	defer tree.Close()

	n := 15000

	for i := 0; i < n; i++ {
		_, err := tree.Read([]byte(fmt.Sprintf("%v", rand.Intn(n))))
		if err != nil {
			slog.Error(fmt.Sprintf("error reading %v", i))
			return
		}
	}
	fmt.Println("Success")
}
