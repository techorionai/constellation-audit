// Package mock has the mocks of the ports for unit tests. Integration tests
// do not use it.
package mock

import (
	"context"
	"sync"
	"time"

	platformstreams "github.com/techorionai/constellation-platform-go/streams"

	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/internal/ports/streams"
)

var (
	_ entries.Repository = (*Entries)(nil)
	_ streams.Streams    = (*Streams)(nil)
)

// Entries is a mock of the store port. A nil function does nothing and gives
// no error.
type Entries struct {
	InsertFunc         func(ctx context.Context, e domain.Entry) (bool, error)
	InsertRejectedFunc func(ctx context.Context, r domain.Rejected) error

	mu       sync.Mutex
	Rejected []domain.Rejected
	// Inserted has the entries of the calls of Insert, also the calls that fail.
	Inserted []domain.Entry
}

// Insert calls InsertFunc.
func (m *Entries) Insert(ctx context.Context, e domain.Entry) (bool, error) {
	m.mu.Lock()
	m.Inserted = append(m.Inserted, e)
	m.mu.Unlock()
	if m.InsertFunc == nil {
		return true, nil
	}
	return m.InsertFunc(ctx, e)
}

// InsertRejected calls InsertRejectedFunc and keeps the rejected entry.
func (m *Entries) InsertRejected(ctx context.Context, r domain.Rejected) error {
	if m.InsertRejectedFunc != nil {
		if err := m.InsertRejectedFunc(ctx, r); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.Rejected = append(m.Rejected, r)
	m.mu.Unlock()
	return nil
}

// Runner is a consumer that runs until the context ends.
type Runner struct {
	App string
}

// Run waits for the end of the context.
func (r *Runner) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Streams is a mock of the streams port. AppsFunc gives the list of producers
// for each call. NewConsumer keeps the apps that got a consumer.
type Streams struct {
	AppsFunc func(ctx context.Context) ([]string, error)
	LagFunc  func(ctx context.Context, app string) (time.Duration, error)

	mu        sync.Mutex
	consumers []string
}

// Apps calls AppsFunc.
func (m *Streams) Apps(ctx context.Context) ([]string, error) { return m.AppsFunc(ctx) }

// Lag calls LagFunc. Without a function, the lag is 0.
func (m *Streams) Lag(ctx context.Context, app string) (time.Duration, error) {
	if m.LagFunc == nil {
		return 0, nil
	}
	return m.LagFunc(ctx, app)
}

// NewConsumer keeps the app and returns a runner that waits for the context.
func (m *Streams) NewConsumer(app string, _ platformstreams.Handler) (streams.Runner, error) {
	m.mu.Lock()
	m.consumers = append(m.consumers, app)
	m.mu.Unlock()
	return &Runner{App: app}, nil
}

// Consumers returns the apps that got a consumer, in the order of the calls.
func (m *Streams) Consumers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.consumers...)
}

var _ entries.KnownApps = (*Known)(nil)

// Known is a mock of the port of the known apps. A nil function does nothing.
type Known struct {
	ListFunc func(ctx context.Context) ([]string, error)
	AddFunc  func(ctx context.Context, app string) error

	mu    sync.Mutex
	added []string
}

// KnownApps calls ListFunc.
func (m *Known) KnownApps(ctx context.Context, _ int) ([]string, error) {
	if m.ListFunc == nil {
		return nil, nil
	}
	return m.ListFunc(ctx)
}

// RememberApp calls AddFunc and keeps the app.
func (m *Known) RememberApp(ctx context.Context, app string) error {
	if m.AddFunc != nil {
		if err := m.AddFunc(ctx, app); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.added = append(m.added, app)
	m.mu.Unlock()
	return nil
}

// Added returns the apps that the intake remembered, in the order of the calls.
func (m *Known) Added() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.added...)
}

var (
	_ entries.Reader        = (*Reader)(nil)
	_ entries.ExportSession = (*Session)(nil)
)

// Reader is a mock of the read port. A nil function does nothing and gives an
// empty result.
type Reader struct {
	SearchFunc      func(ctx context.Context, f domain.Filters, after *domain.Cursor, limit int) ([]domain.StoredEntry, error)
	GetFunc         func(ctx context.Context, id, service string) (domain.StoredEntry, error)
	BeginExportFunc func(ctx context.Context, f domain.Filters) (entries.ExportSession, error)
}

// Search calls SearchFunc.
func (m *Reader) Search(ctx context.Context, f domain.Filters, after *domain.Cursor, limit int) ([]domain.StoredEntry, error) {
	if m.SearchFunc == nil {
		return nil, nil
	}
	return m.SearchFunc(ctx, f, after, limit)
}

// Get calls GetFunc. Without a function, it gives ErrNotFound.
func (m *Reader) Get(ctx context.Context, id, service string) (domain.StoredEntry, error) {
	if m.GetFunc == nil {
		return domain.StoredEntry{}, entries.ErrNotFound
	}
	return m.GetFunc(ctx, id, service)
}

// BeginExport calls BeginExportFunc. Without a function, it gives an empty
// session.
func (m *Reader) BeginExport(ctx context.Context, f domain.Filters) (entries.ExportSession, error) {
	if m.BeginExportFunc == nil {
		return &Session{}, nil
	}
	return m.BeginExportFunc(ctx, f)
}

// Session is a mock of an export session. Batches are the batches that Next
// returns, then an empty batch.
type Session struct {
	CountValue int
	CountErr   error
	NextErr    error
	Batches    [][]domain.StoredEntry

	mu     sync.Mutex
	closed int
}

// Count returns CountValue, but not more than the limit.
func (m *Session) Count(_ context.Context, limit int) (int, error) {
	if m.CountErr != nil {
		return 0, m.CountErr
	}
	return min(m.CountValue, limit), nil
}

// Next returns the next batch.
func (m *Session) Next(context.Context, int) ([]domain.StoredEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.NextErr != nil {
		return nil, m.NextErr
	}
	if len(m.Batches) == 0 {
		return nil, nil
	}
	b := m.Batches[0]
	m.Batches = m.Batches[1:]
	return b, nil
}

// Close counts the calls.
func (m *Session) Close(context.Context) {
	m.mu.Lock()
	m.closed++
	m.mu.Unlock()
}

// Closed returns the number of calls of Close.
func (m *Session) Closed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}
