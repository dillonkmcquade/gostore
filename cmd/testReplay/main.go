package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dillonkmcquade/gostore/internal/lsm_tree"
)

func main() {
	userHome := os.Getenv("HOME")
	gostoreHome := filepath.Join(userHome, ".gostore")

	opts := lsm_tree.NewTestLSMOpts(gostoreHome)
	opts.BloomOpts.Size = 150000
	opts.BloomOpts.NumHashFuncs = 7
	tree := lsm_tree.New[int64, []byte](opts)

	defer tree.Close()

	for i := 0; i < 15500; i++ {
		_, err := tree.Read(int64(i))
		if err != nil {
			log.Fatalf("error reading %v, received '%v'", i, err)
		}
	}
	fmt.Println("Success")
}
