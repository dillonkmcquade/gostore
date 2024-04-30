package tests

import (
	"log/slog"
	"os"
	"testing"

	"github.com/dillonkmcquade/gostore/internal/lsm_tree"
)

const logLevel = slog.LevelDebug

func BenchmarkWrite(b *testing.B) {
	tmp := b.TempDir()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	opts := lsm_tree.NewTestLSMOpts(tmp)
	tree := lsm_tree.New[int64, []byte](opts)
	defer tree.Close()

	for i := 0; i < b.N; i++ {
		err := tree.Write(int64(i), []byte("TESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE"))
		if err != nil {
			b.Error(err)
		}
	}

	for i := 0; i < b.N; i++ {
		_, err := tree.Read(int64(i))
		if err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}
