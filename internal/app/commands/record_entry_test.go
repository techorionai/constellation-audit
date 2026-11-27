package commands_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

const goodEvent = `{"specversion":"1.0","id":"0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88","source":"/helpdesk",` +
	`"type":"helpdesk.audit.recorded","time":"2026-11-27T10:15:00Z","datacontenttype":"application/json",` +
	`"actor":"usr_1","actortype":"user","data":{"action":"helpdesk:ticket:write","result":"success","summary":"SECRET-SUMMARY"}}`

const badEvent = `{"specversion":"1.0","id":"0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88","source":"/helpdesk",` +
	`"type":"helpdesk.audit.recorded","time":"2026-11-27T10:15:00Z","datacontenttype":"application/json",` +
	`"actor":"usr_1","actortype":"user","data":{"action":"crm:ticket:write","result":"success","summary":"SECRET-SUMMARY"}}`

// U4: a store that is not available gives an error. A failed insert into the
// rejected store gives an error. A class 22 error of the store gives a
// rejection with the reason store_refused and no error.
func TestU4_RecordEntry(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	cmd := commands.RecordEntry{Stream: "helpdesk", StreamID: "1-0", Payload: []byte(goodEvent)}

	t.Run("store unavailable", func(t *testing.T) {
		repo := &mock.Entries{InsertFunc: func(context.Context, domain.Entry) (bool, error) {
			return false, entries.ErrUnavailable
		}}
		_, err := commands.NewRecordEntry(repo, log).Execute(context.Background(), cmd)
		if !errors.Is(err, entries.ErrUnavailable) {
			t.Fatalf("error = %v", err)
		}
		if len(repo.Rejected) != 0 {
			t.Fatal("an unavailable store must not reject the entry")
		}
	})

	t.Run("rejected store fails", func(t *testing.T) {
		boom := errors.New("boom")
		repo := &mock.Entries{InsertRejectedFunc: func(context.Context, domain.Rejected) error { return boom }}
		bad := commands.RecordEntry{Stream: "helpdesk", StreamID: "2-0", Payload: []byte(badEvent)}
		if _, err := commands.NewRecordEntry(repo, log).Execute(context.Background(), bad); !errors.Is(err, boom) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("class 22 error", func(t *testing.T) {
		repo := &mock.Entries{InsertFunc: func(context.Context, domain.Entry) (bool, error) {
			return false, entries.ErrDataRefused
		}}
		out, err := commands.NewRecordEntry(repo, log).Execute(context.Background(), cmd)
		if err != nil || out != commands.OutcomeRejected {
			t.Fatalf("outcome = %q, error = %v", out, err)
		}
		if len(repo.Rejected) != 1 || repo.Rejected[0].Reason != domain.ReasonStoreRefused ||
			repo.Rejected[0].Stream != "helpdesk" || repo.Rejected[0].StreamID != "1-0" ||
			repo.Rejected[0].EntryID != "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88" || string(repo.Rejected[0].Payload) != goodEvent {
			t.Fatalf("rejected = %+v", repo.Rejected)
		}
	})

	t.Run("outcomes", func(t *testing.T) {
		inserted := true
		repo := &mock.Entries{InsertFunc: func(context.Context, domain.Entry) (bool, error) { return inserted, nil }}
		h := commands.NewRecordEntry(repo, log)
		if out, err := h.Execute(context.Background(), cmd); err != nil || out != commands.OutcomeStored {
			t.Fatalf("outcome = %q, error = %v", out, err)
		}
		inserted = false
		if out, err := h.Execute(context.Background(), cmd); err != nil || out != commands.OutcomeDuplicate {
			t.Fatalf("outcome = %q, error = %v", out, err)
		}
		bad := commands.RecordEntry{Stream: "helpdesk", StreamID: "3-0", Payload: []byte(badEvent)}
		if out, err := h.Execute(context.Background(), bad); err != nil || out != commands.OutcomeRejected {
			t.Fatalf("outcome = %q, error = %v", out, err)
		}
		if len(repo.Rejected) != 1 || repo.Rejected[0].Reason != domain.ReasonInvalidAction {
			t.Fatalf("rejected = %+v", repo.Rejected)
		}
	})

	if strings.Contains(logs.String(), "SECRET-SUMMARY") || strings.Contains(logs.String(), "usr_1") {
		t.Fatalf("a log record has a value of the entry: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "invalid_action") || !strings.Contains(logs.String(), "stream_id=3-0") {
		t.Fatalf("the log record of a rejected entry has no reason or stream ID: %s", logs.String())
	}
}
