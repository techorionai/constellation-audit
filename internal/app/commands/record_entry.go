// Package commands holds the use cases that change the audit store.
package commands

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// Outcome is the result of the record of one stream message.
type Outcome string

// The outcomes of RecordEntry.
const (
	// OutcomeStored means that the store has a new entry.
	OutcomeStored Outcome = "stored"
	// OutcomeDuplicate means that the store had the entry already.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeRejected means that the message broke a rule and went to the store
	// of rejected entries.
	OutcomeRejected Outcome = "rejected"
)

// RecordEntry is the command that records one message of an audit stream.
type RecordEntry struct {
	// Stream is the app of the stream. It is the trusted service of the entry.
	Stream string
	// StreamID is the ID of the stream message.
	StreamID string
	// Payload is the CloudEvent in JSON.
	Payload []byte
}

// RecordEntryHandler runs RecordEntry.
type RecordEntryHandler struct {
	repo entries.Repository
	log  *slog.Logger
	now  func() time.Time
}

// Option changes the handler. The options are for tests.
type Option func(*RecordEntryHandler)

// WithClock sets the clock of the handler. The default is time.Now.
func WithClock(now func() time.Time) Option { return func(h *RecordEntryHandler) { h.now = now } }

// NewRecordEntry makes the handler. A nil log means slog.Default.
func NewRecordEntry(repo entries.Repository, log *slog.Logger, opts ...Option) *RecordEntryHandler {
	if log == nil {
		log = slog.Default()
	}
	h := &RecordEntryHandler{repo: repo, log: log, now: time.Now}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Execute checks the message and stores it. A message that breaks a rule, and
// a message that the store refuses because of its data, go to the store of
// rejected entries, and the result has no error. A store that is not available,
// and a failed insert into the rejected store, give an error, so the message
// stays pending and comes again.
//
// No log record of this command has a value of the entry. A record of a
// rejected message has the stream, the stream ID, and the reason code.
func (h *RecordEntryHandler) Execute(ctx context.Context, cmd RecordEntry) (Outcome, error) {
	e, err := domain.ParseEntry(cmd.Stream, cmd.Payload, h.now())
	if err != nil {
		reason, ok := domain.AsRejected(err)
		if !ok {
			return "", fmt.Errorf("check entry: %w", err)
		}
		return h.reject(ctx, cmd, reason)
	}
	inserted, err := h.repo.Insert(ctx, e)
	switch {
	case errors.Is(err, entries.ErrDataRefused):
		return h.reject(ctx, cmd, domain.ReasonStoreRefused)
	case err != nil:
		return "", err
	case inserted:
		return OutcomeStored, nil
	default:
		return OutcomeDuplicate, nil
	}
}

func (h *RecordEntryHandler) reject(ctx context.Context, cmd RecordEntry, reason domain.Reason) (Outcome, error) {
	err := h.repo.InsertRejected(ctx, domain.Rejected{
		Stream: cmd.Stream, StreamID: cmd.StreamID, EntryID: domain.EventID(cmd.Payload),
		Reason: reason, Payload: cmd.Payload,
	})
	if err != nil {
		return "", err
	}
	h.log.WarnContext(ctx, "an audit entry was rejected",
		"stream", cmd.Stream, "stream_id", cmd.StreamID, "reason", string(reason))
	return OutcomeRejected, nil
}
