package tests

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/lsm"
)

const logLevel = slog.LevelInfo

func BenchmarkWrite(b *testing.B) {
	tmp := b.TempDir()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	opts := lsm.NewTestLSMOpts(tmp)
	tree, err := lsm.New(opts)
	if err != nil {
		b.Error(err)
	}
	defer tree.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err := tree.Write([]byte(fmt.Sprintf("%v", i)), []byte("TESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE"))
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
	e := b.Elapsed()

	if e > 2*time.Second {
		b.Errorf("Write benchmark exceeded expected time: %v > %v", e, 2*time.Second)
	}
}
