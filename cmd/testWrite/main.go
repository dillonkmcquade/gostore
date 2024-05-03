package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/dillonkmcquade/gostore/internal/lsm_tree"
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile = flag.String("memprofile", "", "write memory profile to `file`")
	logLevel   = new(slog.LevelVar)
)

func main() {
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

	userHome := os.Getenv("HOME")
	gostoreHome := filepath.Join(userHome, ".gostore")

	opts := lsm_tree.NewTestLSMOpts(gostoreHome)
	tree := lsm_tree.New[int64, []uint8](opts)

	defer tree.Close()

	for i := 0; i < 15500; i++ {
		err := tree.Write(int64(i), []byte("TESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE"))
		if err != nil {
			log.Fatal(err)
		}
	}

	// for i := 0; i < 15000; i++ {
	// 	wg.Add(1)
	// 	go func(n int) {
	// 		_, err := tree.Read(int64(n))
	// 		if err != nil {
	// 			slog.Error("Read", "id", n)
	// 			log.Fatal(err)
	// 		}
	// 		wg.Done()
	// 	}(i)
	// }
	// wg.Wait()
}
