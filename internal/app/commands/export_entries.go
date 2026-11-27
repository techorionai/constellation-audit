package commands

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/techorionai/constellation-audit/internal/app/problems"
	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// Export constants.
const (
	// ExportBatch is the number of entries that one read of an export returns.
	// An entry has a maximum of 64 KiB, so one batch has a maximum of 6.4 MiB.
	ExportBatch = 100
	// OwnEntryTimeout is the time limit to store the own audit entry of a
	// request.
	OwnEntryTimeout = 10 * time.Second
	// deniedEvery is the shortest time between two denied entries of one
	// principal.
	deniedEvery = time.Minute
	// maxDeniedPrincipals is the size of the map of the last denied entries.
	// A larger map drops the old values.
	maxDeniedPrincipals = 1024
	// ExportRetryAfter is the Retry-After value of a 429 answer.
	ExportRetryAfter = 10 * time.Second
	// MetricExport is the name of the counter of the exports.
	MetricExport = "constellation.audit.export"
)

// Codes of the problems of an export.
const (
	CodeExportTooLarge  = "export_too_large"
	CodeTooManyExports  = "too_many_exports"
	outcomeOK           = "ok"
	outcomeTooLarge     = "too_large"
	outcomeBusy         = "busy"
	outcomeError        = "error"
	resultSuccess       = "success"
	resultFailure       = "failure"
	resultInvalidExport = "invalid"
	resultDeniedExport  = "denied"
)

// Principal is the verified principal of a request. Act and ActType are empty
// if the token has no act claim.
type Principal struct {
	ID, Type     string
	Act, ActType string
}

// RequestInfo is the data of the request for the own audit entry. The domain
// checks each value.
type RequestInfo struct {
	RequestID string
	TraceID   string
	IPAddress string
	UserAgent string
}

// ExportEntries is the command of an export.
type ExportEntries struct {
	Filters   domain.Filters
	Format    string
	Principal Principal
	Request   RequestInfo
}

// ExportStream is the read of an export that is ready. The caller must call
// Finish, also when it stops early.
type ExportStream struct {
	// Count is the number of entries of the export.
	Count int

	sent int

	session entries.ExportSession
	h       *ExportEntriesHandler
	cmd     ExportEntries
	once    sync.Once
}

// Next returns the next batch of entries. An empty result is the end.
func (s *ExportStream) Next(ctx context.Context) ([]domain.StoredEntry, error) {
	batch, err := s.session.Next(ctx, ExportBatch)
	s.sent += len(batch)
	return batch, err
}

// Finish ends the snapshot, frees the slot, and counts the export. err is the
// error that stopped the body, or nil. If err is not nil, Finish also writes a
// second own audit entry with the result failure and the number of entries
// that the export read before it stopped.
func (s *ExportStream) Finish(ctx context.Context, err error) {
	s.once.Do(func() {
		s.session.Close(ctx)
		outcome := outcomeOK
		if err != nil {
			outcome = outcomeError
			s.h.record(ctx, s.cmd, resultFailure, s.sent)
		}
		s.h.release(s.cmd.Principal.ID)
		s.h.finish(ctx, s.cmd, outcome, s.Count)
	})
}

// ExportEntriesHandler runs ExportEntries.
type ExportEntriesHandler struct {
	reader     entries.Reader
	writer     entries.Repository
	maxEntries int
	slots      chan struct{}
	mu         sync.Mutex
	active     map[string]struct{}
	denied     map[string]time.Time
	log        *slog.Logger
	counter    metric.Int64Counter
	now        func() time.Time
	newID      func() string
}

// ExportOption changes the handler. The options are for tests.
type ExportOption func(*ExportEntriesHandler)

// WithExportClock sets the clock of the handler.
func WithExportClock(now func() time.Time) ExportOption {
	return func(h *ExportEntriesHandler) { h.now = now }
}

// NewExportEntries makes the handler. maxEntries is the largest number of
// entries of an export. concurrency is the number of exports that run at the
// same time. A nil log means slog.Default.
func NewExportEntries(reader entries.Reader, writer entries.Repository, meter metric.Meter, maxEntries, concurrency int, log *slog.Logger, opts ...ExportOption) (*ExportEntriesHandler, error) {
	if log == nil {
		log = slog.Default()
	}
	counter, err := meter.Int64Counter(MetricExport, metric.WithDescription("Exports of audit entries by format and outcome."))
	if err != nil {
		return nil, err
	}
	h := &ExportEntriesHandler{
		reader: reader, writer: writer, maxEntries: maxEntries, slots: make(chan struct{}, concurrency),
		active: map[string]struct{}{}, denied: map[string]time.Time{},
		log: log, counter: counter, now: time.Now,
		newID: func() string {
			id, err := uuid.NewV7()
			if err != nil {
				id = uuid.New()
			}
			return id.String()
		},
	}
	for _, o := range opts {
		o(h)
	}
	return h, nil
}

// Execute checks the limits, writes the own audit entry, and returns the
// stream of the entries. It gives the 429 problem too_many_exports with no free
// slot or if the principal has an export already, the 422 problem export_too_large with too many entries, and the 503
// problem if the store is not available. A 429 and a 422 also write an own
// audit entry, with the result denied and invalid. A principal gets a denied
// entry once in each minute.
//
// No log record of this command has a filter value or an entry.
func (h *ExportEntriesHandler) Execute(ctx context.Context, cmd ExportEntries) (*ExportStream, error) {
	if err := cmd.Filters.Validate(); err != nil {
		return nil, problems.FromFilter(err)
	}
	if !h.acquire(cmd.Principal.ID) {
		h.recordDenied(ctx, cmd)
		h.finish(ctx, cmd, outcomeBusy, 0)
		return nil, apperrors.NewTooManyRequests(CodeTooManyExports, "Too many exports run now. Try again later.", ExportRetryAfter)
	}
	stream, err := h.start(ctx, cmd)
	if err != nil {
		h.release(cmd.Principal.ID)
		return nil, err
	}
	return stream, nil
}

// acquire takes a slot for the principal. It fails if no slot is free or if the
// principal has an export already.
func (h *ExportEntriesHandler) acquire(principal string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.active[principal]; ok {
		return false
	}
	select {
	case h.slots <- struct{}{}:
		h.active[principal] = struct{}{}
		return true
	default:
		return false
	}
}

// release frees the slot of the principal.
func (h *ExportEntriesHandler) release(principal string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.active, principal)
	<-h.slots
}

// recordDenied writes the denied entry, but not more than once in each
// deniedEvery for one principal.
func (h *ExportEntriesHandler) recordDenied(ctx context.Context, cmd ExportEntries) {
	now := h.now()
	h.mu.Lock()
	if last, ok := h.denied[cmd.Principal.ID]; ok && now.Sub(last) < deniedEvery {
		h.mu.Unlock()
		return
	}
	if len(h.denied) >= maxDeniedPrincipals {
		for k, v := range h.denied {
			if now.Sub(v) >= deniedEvery {
				delete(h.denied, k)
			}
		}
		if len(h.denied) >= maxDeniedPrincipals {
			clear(h.denied)
		}
	}
	h.denied[cmd.Principal.ID] = now
	h.mu.Unlock()
	h.record(ctx, cmd, resultDeniedExport, 0)
}

func (h *ExportEntriesHandler) start(ctx context.Context, cmd ExportEntries) (*ExportStream, error) {
	session, err := h.reader.BeginExport(ctx, cmd.Filters)
	if err != nil {
		h.finish(ctx, cmd, outcomeError, 0)
		return nil, problems.FromStore(err)
	}
	count, err := session.Count(ctx, h.maxEntries+1)
	if err != nil {
		session.Close(ctx)
		h.finish(ctx, cmd, outcomeError, 0)
		return nil, problems.FromStore(err)
	}
	if count > h.maxEntries {
		session.Close(ctx)
		h.record(ctx, cmd, resultInvalidExport, 0)
		h.finish(ctx, cmd, outcomeTooLarge, 0)
		return nil, apperrors.NewUnprocessable(CodeExportTooLarge, "The export has too many entries. Use more filters.")
	}
	// The own entry goes into the store before the first byte of data leaves.
	if err := h.write(ctx, cmd, resultSuccess, count); err != nil {
		session.Close(ctx)
		h.finish(ctx, cmd, outcomeError, count)
		// A store that is not available gives 503. Other errors give 500,
		// because a retry cannot fix them.
		if errors.Is(err, entries.ErrUnavailable) {
			return nil, apperrors.NewUnavailable()
		}
		return nil, err
	}
	return &ExportStream{Count: count, session: session, h: h, cmd: cmd}, nil
}

// write stores the own audit entry of an export.
func (h *ExportEntriesHandler) write(ctx context.Context, cmd ExportEntries, result string, count int) error {
	e, err := domain.NewExportEntry(domain.ExportEntryInput{
		EntryID: h.newID(), Time: h.now(), Result: result,
		Actor: cmd.Principal.ID, ActorType: cmd.Principal.Type, Act: cmd.Principal.Act, ActType: cmd.Principal.ActType,
		Format: cmd.Format, Filters: cmd.Filters, Count: count, MaxEntries: h.maxEntries,
		RequestID: cmd.Request.RequestID, TraceID: cmd.Request.TraceID,
		IPAddress: cmd.Request.IPAddress, UserAgent: cmd.Request.UserAgent,
	})
	if err != nil {
		return err
	}
	// The write must finish when the client goes away. It has a time limit,
	// because the export keeps its slot while the write runs.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), OwnEntryTimeout)
	defer cancel()
	_, err = h.writer.Insert(wctx, e)
	return err
}

// record writes the own entry of a request that gives no data. A failure goes
// to the log, and the answer of the request does not change.
func (h *ExportEntriesHandler) record(ctx context.Context, cmd ExportEntries, result string, count int) {
	if err := h.write(ctx, cmd, result, count); err != nil {
		h.log.ErrorContext(ctx, "the audit entry of an export is not stored",
			"result", result, "unavailable", errors.Is(err, entries.ErrUnavailable))
	}
}

// finish counts the export and writes the log record. The record has the
// principal ID, the format, the count, and the outcome.
func (h *ExportEntriesHandler) finish(ctx context.Context, cmd ExportEntries, outcome string, count int) {
	h.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("format", cmd.Format), attribute.String("outcome", outcome)))
	h.log.InfoContext(ctx, "audit export",
		"principal", cmd.Principal.ID, "format", cmd.Format, "count", count, "outcome", outcome)
}
