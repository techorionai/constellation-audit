package commands_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

func exportHandler(t *testing.T, reader *mock.Reader, writer *mock.Entries, maxEntries, concurrency int) *commands.ExportEntriesHandler {
	t.Helper()
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	h, err := commands.NewExportEntries(reader, writer, mp.Meter("test"), maxEntries, concurrency, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

var exportCmd = commands.ExportEntries{
	Format:    "csv",
	Filters:   domain.Filters{Service: "helpdesk"},
	Principal: commands.Principal{ID: "svc_reader", Type: "service"},
	Request:   commands.RequestInfo{RequestID: "req-1"},
}

func wantProblem(t *testing.T, err error, status int, code string) *apperrors.ProblemError {
	t.Helper()
	pe, ok := apperrors.AsProblem(err)
	if !ok || pe.Status != status || pe.Code != code {
		t.Fatalf("error = %v, want %d %s", err, status, code)
	}
	return pe
}

// U6: the export command. Count over the limit, store error at the own entry,
// and no free slot.
func TestU6_ExportCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("count over the limit gives 422 and an invalid entry", func(t *testing.T) {
		session := &mock.Session{CountValue: 3}
		writer := &mock.Entries{}
		h := exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
			return session, nil
		}}, writer, 2, 2)
		_, err := h.Execute(ctx, exportCmd)
		_ = wantProblem(t, err, 422, "export_too_large")
		if len(writer.Inserted) != 1 || writer.Inserted[0].Result != "invalid" || writer.Inserted[0].Action != "audit:log:export" ||
			!strings.Contains(string(writer.Inserted[0].Details), `"maxEntries":2`) {
			t.Fatalf("entries = %+v", writer.Inserted)
		}
		if session.Closed() != 1 {
			t.Fatalf("closed = %d", session.Closed())
		}
	})

	t.Run("store error at the own entry gives 503 and no stream", func(t *testing.T) {
		session := &mock.Session{CountValue: 1, Batches: [][]domain.StoredEntry{{{}}}}
		writer := &mock.Entries{InsertFunc: func(context.Context, domain.Entry) (bool, error) { return false, entries.ErrUnavailable }}
		h := exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
			return session, nil
		}}, writer, 10, 1)
		stream, err := h.Execute(ctx, exportCmd)
		pe := wantProblem(t, err, 503, "unavailable")
		if stream != nil || pe.RetryAfter == 0 || session.Closed() != 1 {
			t.Fatalf("stream = %v, retry = %v, closed = %d", stream, pe.RetryAfter, session.Closed())
		}
		// The slot is free again.
		writer.InsertFunc = nil
		if stream, err = h.Execute(ctx, exportCmd); err != nil {
			t.Fatal(err)
		}
		stream.Finish(ctx, nil)
	})

	t.Run("no free slot gives 429 with Retry-After and a denied entry", func(t *testing.T) {
		writer := &mock.Entries{}
		h := exportHandler(t, &mock.Reader{}, writer, 10, 1)
		stream, err := h.Execute(ctx, exportCmd)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.Execute(ctx, exportCmd)
		pe := wantProblem(t, err, 429, "too_many_exports")
		if pe.RetryAfter != 10*time.Second {
			t.Fatalf("retry after = %v", pe.RetryAfter)
		}
		if len(writer.Inserted) != 2 || writer.Inserted[0].Result != "success" || writer.Inserted[1].Result != "denied" {
			t.Fatalf("entries = %+v", writer.Inserted)
		}
		stream.Finish(ctx, nil)
		stream.Finish(ctx, nil) // A second call changes nothing.
		again, err := h.Execute(ctx, exportCmd)
		if err != nil {
			t.Fatalf("the slot is not free after Finish: %v", err)
		}
		again.Finish(ctx, nil)
	})

	t.Run("a success entry has a UUIDv7 and the data of the request", func(t *testing.T) {
		writer := &mock.Entries{}
		h := exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
			return &mock.Session{CountValue: 2}, nil
		}}, writer, 10, 1)
		stream, err := h.Execute(ctx, exportCmd)
		if err != nil {
			t.Fatal(err)
		}
		stream.Finish(ctx, nil)
		e := writer.Inserted[0]
		id, perr := uuid.Parse(e.ID)
		if perr != nil || id.Version() != 7 || e.ID != strings.ToLower(e.ID) {
			t.Fatalf("entry id = %q", e.ID)
		}
		if stream.Count != 2 || e.Result != "success" || e.RequestID != "req-1" || e.Actor != "svc_reader" ||
			!strings.Contains(string(e.Details), `"count":2`) || !strings.Contains(string(e.Details), `"service":"helpdesk"`) {
			t.Fatalf("count = %d, entry = %+v", stream.Count, e)
		}
	})

	t.Run("store errors at the start", func(t *testing.T) {
		h := exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
			return nil, entries.ErrUnavailable
		}}, &mock.Entries{}, 10, 1)
		_, err := h.Execute(ctx, exportCmd)
		_ = wantProblem(t, err, 503, "unavailable")

		h = exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
			return &mock.Session{CountErr: errors.Join(entries.ErrDataRefused)}, nil
		}}, &mock.Entries{}, 10, 1)
		_, err = h.Execute(ctx, exportCmd)
		_ = wantProblem(t, err, 400, "invalid_parameter")
	})
}

// R2: the own entry insert has a time limit.
func TestOwnEntryHasTimeout(t *testing.T) {
	var limited bool
	writer := &mock.Entries{InsertFunc: func(ctx context.Context, _ domain.Entry) (bool, error) {
		_, limited = ctx.Deadline()
		return true, nil
	}}
	h := exportHandler(t, &mock.Reader{}, writer, 10, 1)
	stream, err := h.Execute(context.Background(), exportCmd)
	if err != nil {
		t.Fatal(err)
	}
	stream.Finish(context.Background(), nil)
	if !limited {
		t.Fatal("the insert has no time limit")
	}
}

// R9: an error of the own entry that is not "unavailable" gives no 503.
func TestOwnEntryErrorIsNot503(t *testing.T) {
	writer := &mock.Entries{InsertFunc: func(context.Context, domain.Entry) (bool, error) { return false, errors.New("bad data") }}
	h := exportHandler(t, &mock.Reader{}, writer, 10, 1)
	_, err := h.Execute(context.Background(), exportCmd)
	if err == nil {
		t.Fatal("no error")
	}
	if _, ok := apperrors.AsProblem(err); ok {
		t.Fatalf("error = %v, want an error that is not a problem (500)", err)
	}
}

// R4: a body that stops writes a second entry with the result failure and the
// number of entries that the export read.
func TestFailureEntry(t *testing.T) {
	ctx := context.Background()
	writer := &mock.Entries{}
	session := &mock.Session{CountValue: 5, Batches: [][]domain.StoredEntry{{{}, {}}, {{}}}}
	h := exportHandler(t, &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
		return session, nil
	}}, writer, 10, 1)
	stream, err := h.Execute(ctx, exportCmd)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := stream.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}
	stream.Finish(ctx, errors.New("client gone"))
	if len(writer.Inserted) != 2 || writer.Inserted[0].Result != "success" || writer.Inserted[1].Result != "failure" ||
		!strings.Contains(string(writer.Inserted[1].Details), `"count":3`) {
		t.Fatalf("entries = %+v", writer.Inserted)
	}
}

// R7: one export for each principal, and one denied entry in each minute.
func TestOnePrincipalOneExport(t *testing.T) {
	ctx := context.Background()
	writer := &mock.Entries{}
	h := exportHandler(t, &mock.Reader{}, writer, 10, 3)
	first, err := h.Execute(ctx, exportCmd)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_, err = h.Execute(ctx, exportCmd)
		_ = wantProblem(t, err, 429, "too_many_exports")
	}
	denied := 0
	for _, e := range writer.Inserted {
		if e.Result == "denied" {
			denied++
		}
	}
	if denied != 1 {
		t.Fatalf("denied entries = %d, want 1", denied)
	}
	// Another principal has a free slot.
	other := exportCmd
	other.Principal = commands.Principal{ID: "svc_other", Type: "service"}
	second, err := h.Execute(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	second.Finish(ctx, nil)
	first.Finish(ctx, nil)
	again, err := h.Execute(ctx, exportCmd)
	if err != nil {
		t.Fatalf("the principal has no free slot after Finish: %v", err)
	}
	again.Finish(ctx, nil)
}
