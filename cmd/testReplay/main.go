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
		val, err := tree.Read(int64(i))
		if err != nil || val != fmt.Sprintf("%dtest", i) {
			log.Fatalf("%v Should be %s", i, fmt.Sprintf("%dtest", i))
		}
		fmt.Println(val)
	}
}
