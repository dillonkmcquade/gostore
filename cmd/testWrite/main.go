package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/lsm"
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile = flag.String("memprofile", "", "write memory profile to `file`")
	logLevel   = new(slog.LevelVar)
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	logLevel.Set(slog.LevelDebug)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	userHome := os.Getenv("HOME")
	gostoreHome := filepath.Join(userHome, ".gostore")

	opts := lsm.NewTestLSMOpts(gostoreHome)
	tree, err := lsm.New(opts)
	if err != nil {
		panic(err)
	}

	defer tree.Close()

	var wg sync.WaitGroup
	for i := 0; i < 150000; i++ {
		wg.Add(1)
		go func(i int) {
			err := tree.Write([]byte(fmt.Sprintf("%v", i)), []byte("Hello world"))
			if err != nil {
				slog.Error(err.Error())
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
