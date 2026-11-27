package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-audit/internal/domain"
)

func exportInput() domain.ExportEntryInput {
	return domain.ExportEntryInput{
		EntryID: "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88", Time: time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC),
		Result: "success", Actor: "svc_reader", ActorType: "service", Act: "usr_1", ActType: "user",
		Format: "csv", Filters: domain.Filters{Service: "helpdesk", From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}, Count: 3,
		RequestID: "abc-1", TraceID: strings.Repeat("a", 32), IPAddress: "203.0.113.9", UserAgent: "test-agent",
	}
}

// U9: the own entry has all columns. A value that breaks its rule is left out.
func TestU9_OwnEntry(t *testing.T) {
	e, err := domain.NewExportEntry(exportInput())
	if err != nil {
		t.Fatal(err)
	}
	if e.Service != "audit" || e.Action != "audit:log:export" || e.Result != "success" || e.Summary != "Export of audit entries" {
		t.Fatalf("entry = %+v", e)
	}
	if e.Actor != "svc_reader" || e.ActorType != "service" || e.Act != "usr_1" || e.ActType != "user" {
		t.Fatalf("actors = %+v", e)
	}
	if e.RequestID != "abc-1" || e.TraceID != strings.Repeat("a", 32) || e.IPAddress != "203.0.113.9" || e.UserAgent != "test-agent" {
		t.Fatalf("request data = %+v", e)
	}
	if e.TargetType != "" || e.TargetID != "" || e.ID != "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88" {
		t.Fatalf("entry = %+v", e)
	}
	var d struct {
		Format  string            `json:"format"`
		Filters map[string]string `json:"filters"`
		Count   *int              `json:"count"`
		Max     *int              `json:"maxEntries"`
	}
	if err := json.Unmarshal(e.Details, &d); err != nil {
		t.Fatal(err)
	}
	if d.Format != "csv" || d.Count == nil || *d.Count != 3 || d.Max != nil ||
		len(d.Filters) != 2 || d.Filters["service"] != "helpdesk" || d.Filters["from"] != "2026-11-01T00:00:00Z" {
		t.Fatalf("details = %s", e.Details)
	}

	in := exportInput()
	in.RequestID, in.IPAddress, in.UserAgent, in.TraceID = "a b", "fe80::1%eth0", "a\x00b", ""
	in.Act, in.ActType = "", ""
	e, err = domain.NewExportEntry(in)
	if err != nil {
		t.Fatal(err)
	}
	if e.RequestID != "" || e.IPAddress != "" || e.UserAgent != "" || e.TraceID != "" || e.Act != "" {
		t.Fatalf("a bad value is in the entry: %+v", e)
	}

	in = exportInput()
	in.Result, in.MaxEntries = "invalid", 2
	e, err = domain.NewExportEntry(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(e.Details), `"maxEntries":2`) || strings.Contains(string(e.Details), `"count"`) {
		t.Fatalf("details of a 422 = %s", e.Details)
	}
}
