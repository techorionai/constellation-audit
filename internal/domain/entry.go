// Package domain holds the audit entry and the checks of the audit contract.
// It imports the standard library only.
package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/netip"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits of the contract.
const (
	// MaxEntryBytes is the maximum size of the CloudEvent in JSON.
	MaxEntryBytes   = 64 << 10
	maxDetailsBytes = 16 << 10
	maxActionLen    = 128
	maxActorLen     = 128
	maxTargetIDLen  = 256
	maxSummaryLen   = 1024
	maxRequestIDLen = 128
	maxIPLen        = 45
	maxUserAgentLen = 512
	maxEventIDLen   = 128
	specVersion     = "1.0"
	typeSuffix      = ".audit.recorded"
)

var (
	idPattern          = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	actionPattern      = regexp.MustCompile(`^(?:[a-z][a-z0-9_]*:[a-z][a-z0-9_]*:[a-z][a-z0-9_]*|identity:admin)$`)
	targetTypePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$`)
)

// Reason is the code of a rule that an entry breaks.
type Reason string

// The reason codes of a rejected entry.
const (
	ReasonTooLarge           Reason = "too_large"
	ReasonInvalidJSON        Reason = "invalid_json"
	ReasonInvalidSpecVersion Reason = "invalid_specversion"
	ReasonInvalidSource      Reason = "invalid_source"
	ReasonInvalidType        Reason = "invalid_type"
	ReasonInvalidID          Reason = "invalid_id"
	ReasonInvalidTime        Reason = "invalid_time"
	ReasonInvalidActor       Reason = "invalid_actor"
	ReasonInvalidAct         Reason = "invalid_act"
	ReasonInvalidTraceparent Reason = "invalid_traceparent"
	ReasonInvalidAction      Reason = "invalid_action"
	ReasonInvalidResult      Reason = "invalid_result"
	ReasonInvalidTarget      Reason = "invalid_target"
	ReasonInvalidSummary     Reason = "invalid_summary"
	ReasonInvalidDetails     Reason = "invalid_details"
	ReasonInvalidRequestID   Reason = "invalid_request_id"
	ReasonInvalidIPAddress   Reason = "invalid_ip_address"
	ReasonInvalidUserAgent   Reason = "invalid_user_agent"
	ReasonInvalidText        Reason = "invalid_text"
	ReasonStoreRefused       Reason = "store_refused"
)

// RejectedError is the error of an entry that breaks a rule of the contract.
// The text has the reason code only, never a value of the entry.
type RejectedError struct {
	Reason Reason
}

// Error returns the reason code.
func (e *RejectedError) Error() string { return "entry rejected: " + string(e.Reason) }

// AsRejected returns the reason of a RejectedError in the chain.
func AsRejected(err error) (Reason, bool) {
	var re *RejectedError
	if errors.As(err, &re) {
		return re.Reason, true
	}
	return "", false
}

func reject(r Reason) error { return &RejectedError{Reason: r} }

// Entry is one audit entry, as the store keeps it.
type Entry struct {
	// ID is the CloudEvent ID, a UUID in lowercase text form.
	ID string
	// Service is the app of the stream.
	Service string
	// Time is the time of the action.
	Time      time.Time
	Action    string
	Result    string
	Actor     string
	ActorType string
	// Act and ActType are empty if not set.
	Act     string
	ActType string
	// TargetType and TargetID are empty if not set.
	TargetType string
	TargetID   string
	Summary    string
	// Details is a JSON object, or nil if not set.
	Details   []byte
	RequestID string
	TraceID   string
	// IPAddress is the text form of the address, or empty.
	IPAddress string
	UserAgent string
}

type envelope struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Source      string          `json:"source"`
	Type        string          `json:"type"`
	Time        *string         `json:"time"`
	Data        json.RawMessage `json:"data"`
	Actor       *string         `json:"actor"`
	ActorType   string          `json:"actortype"`
	Act         *string         `json:"act"`
	ActType     *string         `json:"acttype"`
	Traceparent *string         `json:"traceparent"`
}

type targetData struct {
	Type *string `json:"type"`
	ID   *string `json:"id"`
}

type entryData struct {
	Action    string          `json:"action"`
	Result    string          `json:"result"`
	Target    json.RawMessage `json:"target"`
	Summary   *string         `json:"summary"`
	Details   json.RawMessage `json:"details"`
	RequestID *string         `json:"requestId"`
	IPAddress *string         `json:"ipAddress"`
	UserAgent *string         `json:"userAgent"`
}

func validPrincipalType(s string) bool {
	switch s {
	case "user", "agent", "service", "workflow":
		return true
	}
	return false
}

func runes(s string) int { return utf8.RuneCountInString(s) }

// ParseEntry checks the CloudEvent of a stream message and makes the entry. app
// is the app of the stream key. It is the trusted source of the service. The
// error is a *RejectedError with the reason code when the entry breaks a rule.
// The decoder ignores unknown members of the envelope and of data.
func ParseEntry(app string, payload []byte) (Entry, error) {
	if len(payload) > MaxEntryBytes {
		return Entry{}, reject(ReasonTooLarge)
	}
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Entry{}, reject(ReasonInvalidJSON)
	}
	var data entryData
	if len(env.Data) == 0 || json.Unmarshal(env.Data, &data) != nil {
		return Entry{}, reject(ReasonInvalidJSON)
	}
	if env.SpecVersion != specVersion {
		return Entry{}, reject(ReasonInvalidSpecVersion)
	}
	if env.Source != "/"+app {
		return Entry{}, reject(ReasonInvalidSource)
	}
	if env.Type != app+typeSuffix {
		return Entry{}, reject(ReasonInvalidType)
	}
	if !idPattern.MatchString(env.ID) {
		return Entry{}, reject(ReasonInvalidID)
	}
	if env.Time == nil {
		return Entry{}, reject(ReasonInvalidTime)
	}
	t, err := time.Parse(time.RFC3339Nano, *env.Time)
	if err != nil {
		return Entry{}, reject(ReasonInvalidTime)
	}

	e := Entry{ID: env.ID, Service: app, Time: t.UTC(), Action: data.Action, Result: data.Result}
	if err := parseActors(&e, &env); err != nil {
		return Entry{}, err
	}
	if env.Traceparent != nil {
		m := traceparentPattern.FindStringSubmatch(*env.Traceparent)
		if m == nil {
			return Entry{}, reject(ReasonInvalidTraceparent)
		}
		e.TraceID = m[1]
	}
	if err := parseData(&e, &data, app); err != nil {
		return Entry{}, err
	}
	if hasNUL(&e) {
		return Entry{}, reject(ReasonInvalidText)
	}
	return e, nil
}

func parseActors(e *Entry, env *envelope) error {
	if env.Actor == nil || runes(*env.Actor) < 1 || runes(*env.Actor) > maxActorLen || !validPrincipalType(env.ActorType) {
		return reject(ReasonInvalidActor)
	}
	e.Actor, e.ActorType = *env.Actor, env.ActorType
	if (env.Act == nil) != (env.ActType == nil) {
		return reject(ReasonInvalidAct)
	}
	if env.Act != nil {
		if runes(*env.Act) < 1 || runes(*env.Act) > maxActorLen || !validPrincipalType(*env.ActType) {
			return reject(ReasonInvalidAct)
		}
		e.Act, e.ActType = *env.Act, *env.ActType
	}
	return nil
}

func isNull(raw json.RawMessage) bool { return len(raw) == 0 || bytes.Equal(raw, []byte("null")) }

func parseData(e *Entry, d *entryData, app string) error {
	prefix := app + ":"
	if len(d.Action) <= len(prefix) || len(d.Action) > maxActionLen || !strings.HasPrefix(d.Action, prefix) ||
		!actionPattern.MatchString(d.Action) {
		return reject(ReasonInvalidAction)
	}
	switch d.Result {
	case "success", "denied", "invalid", "failure":
	default:
		return reject(ReasonInvalidResult)
	}
	if !isNull(d.Target) {
		var t targetData
		if json.Unmarshal(d.Target, &t) != nil || t.Type == nil || t.ID == nil ||
			!targetTypePattern.MatchString(*t.Type) || runes(*t.ID) < 1 || runes(*t.ID) > maxTargetIDLen {
			return reject(ReasonInvalidTarget)
		}
		e.TargetType, e.TargetID = *t.Type, *t.ID
	}
	var err error
	if e.Summary, err = optional(d.Summary, 1, maxSummaryLen, ReasonInvalidSummary); err != nil {
		return err
	}
	if !isNull(d.Details) {
		raw := bytes.TrimSpace(d.Details)
		if len(raw) == 0 || raw[0] != '{' {
			return reject(ReasonInvalidDetails)
		}
		var buf bytes.Buffer
		if json.Compact(&buf, raw) != nil || buf.Len() > maxDetailsBytes {
			return reject(ReasonInvalidDetails)
		}
		if HasEscapedNUL(buf.Bytes()) {
			return reject(ReasonInvalidText)
		}
		e.Details = buf.Bytes()
	}
	if e.RequestID, err = optional(d.RequestID, 1, maxRequestIDLen, ReasonInvalidRequestID); err != nil {
		return err
	}
	if d.IPAddress != nil {
		if e.IPAddress, err = parseIP(*d.IPAddress); err != nil {
			return err
		}
	}
	if e.UserAgent, err = optional(d.UserAgent, 1, maxUserAgentLen, ReasonInvalidUserAgent); err != nil {
		return err
	}
	return nil
}

// optional checks the length in characters of a value that is not required.
func optional(v *string, minLen, maxLen int, r Reason) (string, error) {
	if v == nil {
		return "", nil
	}
	if runes(*v) < minLen || runes(*v) > maxLen {
		return "", reject(r)
	}
	return *v, nil
}

// parseIP checks an IPv4 or IPv6 address in text form, with no port and no
// zone. It returns the canonical text form.
func parseIP(s string) (string, error) {
	if len(s) < 1 || len(s) > maxIPLen {
		return "", reject(ReasonInvalidIPAddress)
	}
	a, err := netip.ParseAddr(s)
	if err != nil || a.Zone() != "" {
		return "", reject(ReasonInvalidIPAddress)
	}
	return a.String(), nil
}

// hasNUL reports whether a text field has the character U+0000. PostgreSQL
// text refuses it.
func hasNUL(e *Entry) bool {
	for _, s := range []string{e.Actor, e.Act, e.TargetID, e.Summary, e.RequestID, e.UserAgent} {
		if strings.IndexByte(s, 0) >= 0 {
			return true
		}
	}
	return false
}

// HasEscapedNUL reports whether JSON text has the escape \u0000 outside of an
// escaped backslash. PostgreSQL jsonb refuses it.
func HasEscapedNUL(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		if i+5 < len(b) && b[i+1] == 'u' && string(b[i+2:i+6]) == "0000" {
			return true
		}
		i++
	}
	return false
}

// Rejected is an entry that failed a check and goes to the rejected store.
type Rejected struct {
	// Stream is the app of the stream.
	Stream string
	// StreamID is the ID of the stream message.
	StreamID string
	// EntryID is the CloudEvent ID, or empty if the payload has none.
	EntryID string
	Reason  Reason
	Payload []byte
}

// EventID reads the id member of a payload for the rejected store. It returns
// an empty string if the payload has no usable id. The value has a maximum of
// 128 bytes and has no U+0000.
func EventID(payload []byte) string {
	var v struct {
		ID string `json:"id"`
	}
	if len(payload) > MaxEntryBytes || json.Unmarshal(payload, &v) != nil || len(v.ID) > maxEventIDLen || strings.IndexByte(v.ID, 0) >= 0 {
		return ""
	}
	return v.ID
}
