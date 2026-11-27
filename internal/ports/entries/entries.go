// Package entries is the port to the store of audit entries and rejected
// entries.
package entries

import (
	"context"
	"errors"

	"github.com/techorionai/constellation-audit/internal/domain"
)

var (
	// ErrUnavailable means that the store cannot serve requests now. A retry
	// can work.
	ErrUnavailable = errors.New("entries: the store is not available")
	// ErrDataRefused means that the store refused the data of the entry, for
	// example a character that the store cannot keep. A retry cannot work.
	ErrDataRefused = errors.New("entries: the store refused the data")
)

// Repository is the append-only store.
type Repository interface {
	// Insert stores an entry. It returns false if the store has the entry
	// already (same service and entry ID).
	Insert(ctx context.Context, e domain.Entry) (inserted bool, err error)
	// InsertRejected stores a rejected entry. A second call with the same
	// stream and stream ID changes nothing.
	InsertRejected(ctx context.Context, r domain.Rejected) error
}
