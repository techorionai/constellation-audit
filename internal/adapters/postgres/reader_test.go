package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// R3: a read has a time limit, and a read that waits for a free slot too long
// gives ErrUnavailable.
func TestBeginReadLimits(t *testing.T) {
	r := New(nil)
	ctx, done, err := r.beginRead(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("the read has no time limit")
	}
	done()

	var dones []func()
	for range MaxParallelReads {
		_, d, err := r.beginRead(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}
		dones = append(dones, d)
	}
	waiting, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// The parent context ends first, so the wait stops early.
	if _, _, err := r.beginRead(waiting, "test"); !errors.Is(err, entries.ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	dones[0]()
	if _, d, err := r.beginRead(context.Background(), "test"); err != nil {
		t.Fatalf("no free slot after done: %v", err)
	} else {
		d()
	}
	for _, d := range dones[1:] {
		d()
	}
}
