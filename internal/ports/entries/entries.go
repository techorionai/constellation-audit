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

// KnownApps keeps the apps whose audit streams the intake found. The store is
// durable, so a restart of the service does not lose the apps.
type KnownApps interface {
	// KnownApps returns the sorted apps, a maximum of limit.
	KnownApps(ctx context.Context, limit int) ([]string, error)
	// RememberApp keeps an app. A second call with the same app changes nothing.
	RememberApp(ctx context.Context, app string) error
}

// ErrNotFound means that the store has no entry for the key.
var ErrNotFound = errors.New("entries: the entry does not exist")

// Reader reads the entries. The order of all reads is time descending, then
// the internal key descending.
type Reader interface {
	// Search returns a maximum of limit entries that match the filters and are
	// after the cursor. A nil cursor starts at the newest entry.
	Search(ctx context.Context, f domain.Filters, after *domain.Cursor, limit int) ([]domain.StoredEntry, error)
	// Get returns one entry. If service is empty, it returns the entry with the
	// oldest receipt time. ErrNotFound means that there is no entry.
	Get(ctx context.Context, id, service string) (domain.StoredEntry, error)
	// BeginExport starts the read of an export in one read-only snapshot.
	BeginExport(ctx context.Context, f domain.Filters) (ExportSession, error)
}

// ExportSession reads the entries of an export from one snapshot. The caller
// must call Close.
type ExportSession interface {
	// Count returns the number of matching entries, but not more than limit.
	// It must be the first call.
	Count(ctx context.Context, limit int) (int, error)
	// Next returns the next entries, a maximum of n. An empty result is the end.
	Next(ctx context.Context, n int) ([]domain.StoredEntry, error)
	// Close ends the snapshot.
	Close(ctx context.Context)
}
