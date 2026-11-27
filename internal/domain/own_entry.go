package domain

import (
	"encoding/json"
	"regexp"
	"time"
	"unicode/utf8"
)

// The service and the action of the own audit entry of an export.
const (
	// OwnService is the service of the entries that the audit service writes.
	OwnService = "audit"
	// ActionExport is the action of the entry of an export.
	ActionExport = "audit:log:export"
	// ExportSummary is the summary of the entry of an export.
	ExportSummary = "Export of audit entries"
)

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// ExportEntryInput is the data of the own audit entry of an export.
type ExportEntryInput struct {
	// EntryID is a UUID in lowercase text form.
	EntryID string
	// Time is the time of the decision.
	Time time.Time
	// Result is success, failure (the body stopped), invalid (a 422), or denied
	// (a 429).
	Result string
	// Actor and ActorType are the verified principal. Act and ActType are the
	// act principal of the token, or empty.
	Actor, ActorType string
	Act, ActType     string
	Format           string
	Filters          Filters
	// Count is the number of entries of a successful export, or the number of
	// entries that a failed export read. MaxEntries is the limit that a 422
	// names.
	Count      int
	MaxEntries int
	// The request data. A value that breaks its rule is left out.
	RequestID string
	TraceID   string
	IPAddress string
	UserAgent string
}

// NewExportEntry makes the own audit entry of an export. A request value that
// breaks its rule is left out and does not stop the export.
func NewExportEntry(in ExportEntryInput) (Entry, error) {
	details := map[string]any{"format": in.Format, "filters": in.Filters.Set()}
	switch in.Result {
	case "success", "failure":
		details["count"] = in.Count
	case "invalid":
		details["maxEntries"] = in.MaxEntries
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return Entry{}, err
	}
	e := Entry{
		ID: in.EntryID, Service: OwnService, Time: in.Time.UTC(), Action: ActionExport, Result: in.Result,
		Actor: in.Actor, ActorType: in.ActorType, Act: in.Act, ActType: in.ActType,
		Summary: ExportSummary, Details: raw,
	}
	if requestIDPattern.MatchString(in.RequestID) {
		e.RequestID = in.RequestID
	}
	if traceIDPattern.MatchString(in.TraceID) {
		e.TraceID = in.TraceID
	}
	if ip, err := parseIP(in.IPAddress); err == nil {
		e.IPAddress = ip
	}
	if ua := in.UserAgent; ua != "" && validText(ua) && utf8.RuneCountInString(ua) <= maxUserAgentLen {
		e.UserAgent = ua
	}
	return e, nil
}
