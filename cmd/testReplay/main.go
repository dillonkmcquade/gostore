package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dillonkmcquade/gostore/internal/lsm_tree"
)

var logLevel = new(slog.LevelVar)

func main() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered", "cause", r)
		}
	}()
	logLevel.Set(slog.LevelInfo)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	userHome := os.Getenv("HOME")
	gostoreHome := filepath.Join(userHome, ".gostore")

	opts := lsm_tree.NewTestLSMOpts(gostoreHome)
	tree := lsm_tree.New[int64, []byte](opts)

	defer tree.Close()

	for i := 0; i < 15500; i++ {
		v, err := tree.Read(int64(i))
		if err != nil {
			log.Fatalf("error reading %v, received '%v'", i, err)
		}
		fmt.Println(string(v))
	}
	fmt.Println("Success")
}
