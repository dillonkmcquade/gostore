package main

import (
	"fmt"
	"log"

	"github.com/dillonkmcquade/gostore/internal/lsm_tree"
)

func main() {
	tree := lsm_tree.New[int64, string](200)
	defer tree.Close()

	for i := 0; i < 10; i++ {
		err := tree.Write(int64(i), fmt.Sprintf("%dtest", i))
		if err != nil {
			log.Fatal(err)
		}
	}
}
