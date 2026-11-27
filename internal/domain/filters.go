package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits of the filters of the contract.
const (
	maxServiceLen = 32
	maxFilterText = 128
)

var (
	servicePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// FilterError is the error of a filter value that breaks a rule of the
// contract. It names the parameter. It never holds the value.
type FilterError struct {
	// Param is the name of the query parameter.
	Param string
}

// Error returns the name of the parameter.
func (e *FilterError) Error() string { return "invalid filter: " + e.Param }

func badFilter(param string) error { return &FilterError{Param: param} }

// Filters are the search filters of the contract. An empty value (or a zero
// time) means that the filter is not set.
type Filters struct {
	// From is inclusive. To is exclusive.
	From, To   time.Time
	Service    string
	Action     string
	Result     string
	Actor      string
	ActorType  string
	Act        string
	TargetType string
	TargetID   string
	RequestID  string
	TraceID    string
}

// validText reports whether a text value is valid UTF-8 with no U+0000.
func validText(s string) bool {
	return utf8.ValidString(s) && strings.IndexByte(s, 0) < 0
}

// textLen checks the length in characters of a text filter.
func textLen(s string, minLen, maxLen int) bool {
	n := utf8.RuneCountInString(s)
	return n >= minLen && n <= maxLen
}

// ValidService reports whether s is a valid app key of the contract. It is
// also the rule for the service parameter of the get operation.
func ValidService(s string) bool {
	return len(s) <= maxServiceLen && servicePattern.MatchString(s)
}

// Validate checks the filters. The error is a *FilterError.
func (f Filters) Validate() error {
	if !f.From.IsZero() && !f.To.IsZero() && !f.From.Before(f.To) {
		return badFilter("from")
	}
	if f.Service != "" && !ValidService(f.Service) {
		return badFilter("service")
	}
	if f.Action != "" && (len(f.Action) > maxActionLen || !actionPattern.MatchString(f.Action)) {
		return badFilter("action")
	}
	switch f.Result {
	case "", "success", "denied", "invalid", "failure":
	default:
		return badFilter("result")
	}
	if f.ActorType != "" && !validPrincipalType(f.ActorType) {
		return badFilter("actorType")
	}
	if f.TargetType != "" && !targetTypePattern.MatchString(f.TargetType) {
		return badFilter("targetType")
	}
	if f.TraceID != "" && !traceIDPattern.MatchString(f.TraceID) {
		return badFilter("traceId")
	}
	for _, c := range []struct {
		param, value string
		maxLen       int
	}{
		{"actor", f.Actor, maxFilterText},
		{"act", f.Act, maxFilterText},
		{"targetId", f.TargetID, maxTargetIDLen},
		{"requestId", f.RequestID, maxRequestIDLen},
	} {
		if c.value == "" {
			continue
		}
		if !validText(c.value) || !textLen(c.value, 1, c.maxLen) {
			return badFilter(c.param)
		}
	}
	if f.TargetID != "" && f.TargetType == "" {
		return badFilter("targetId")
	}
	return nil
}

// Set returns the set filters by parameter name. Times are in RFC 3339 form in
// UTC. The result is a new map.
func (f Filters) Set() map[string]string {
	m := map[string]string{}
	set := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	if !f.From.IsZero() {
		set("from", f.From.UTC().Format(time.RFC3339Nano))
	}
	if !f.To.IsZero() {
		set("to", f.To.UTC().Format(time.RFC3339Nano))
	}
	set("service", f.Service)
	set("action", f.Action)
	set("result", f.Result)
	set("actor", f.Actor)
	set("actorType", f.ActorType)
	set("act", f.Act)
	set("targetType", f.TargetType)
	set("targetId", f.TargetID)
	set("requestId", f.RequestID)
	set("traceId", f.TraceID)
	return m
}

// Hash returns the SHA-256 of the canonical JSON of the filters, in hex. The
// same filters give the same hash.
func (f Filters) Hash() string {
	b, _ := json.Marshal(f.Set()) // A map of strings always encodes. The keys are sorted.
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Cursor is the position after the last entry of a page: the time and the
// internal key of that entry. The next page has the entries that are after it
// in the order (time descending, then internal key descending).
type Cursor struct {
	Time time.Time
	ID   int64
}

// StoredEntry is an entry as the store returns it.
type StoredEntry struct {
	Entry
	// Seq is the internal key of the row.
	Seq        int64
	ReceivedAt time.Time
}

// overheadBytes is the size of the JSON keys, the times, and the punctuation of
// one entry, rounded up.
const overheadBytes = 768

// jsonStringLen returns an upper bound of the size of s as a JSON string with
// no quotes.
func jsonStringLen(s string) int {
	n := 0
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			n += 2
		case r < 0x20 || r == '<' || r == '>' || r == '&' || r == 0x2028 || r == 0x2029 || r == utf8.RuneError:
			n += 6
		default:
			n += utf8.RuneLen(r)
		}
	}
	return n
}

// ApproxSize returns an upper bound of the size of the entry in the JSON of
// the API.
func (e StoredEntry) ApproxSize() int {
	n := overheadBytes
	for _, s := range []string{e.ID, e.Service, e.Action, e.Result, e.Actor, e.ActorType, e.Act, e.ActType,
		e.TargetType, e.TargetID, e.Summary, e.RequestID, e.TraceID, e.IPAddress, e.UserAgent, string(e.Details)} {
		n += jsonStringLen(s)
	}
	return n
}
