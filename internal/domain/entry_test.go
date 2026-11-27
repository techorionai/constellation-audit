package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/techorionai/constellation-audit/internal/domain"
)

const (
	validID     = "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88"
	traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)

// event builds a valid CloudEvent for the app helpdesk. A change function can
// break it.
func event(t *testing.T, change func(env, data map[string]any)) []byte {
	t.Helper()
	data := map[string]any{"action": "helpdesk:ticket:write", "result": "success"}
	env := map[string]any{
		"specversion": "1.0", "id": validID, "source": "/helpdesk", "type": "helpdesk.audit.recorded",
		"time": "2026-11-27T10:15:00Z", "datacontenttype": "application/json",
		"actor": "usr_1", "actortype": "user",
	}
	if change != nil {
		change(env, data)
	}
	env["data"] = data
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func reasonOf(t *testing.T, err error) domain.Reason {
	t.Helper()
	r, ok := domain.AsRejected(err)
	if !ok {
		t.Fatalf("error = %v, want a rejected error", err)
	}
	return r
}

// U1: a full entry and a minimal entry pass. The row has all fields, and the
// trace ID is the trace ID of the traceparent.
func TestU1_ValidEntries(t *testing.T) {
	full := event(t, func(env, data map[string]any) {
		env["act"], env["acttype"], env["traceparent"] = "agt_1", "agent", traceparent
		data["target"] = map[string]any{"type": "ticket", "id": "t_1"}
		data["summary"] = "Ticket changed"
		data["details"] = map[string]any{"old": "a", "new": "b"}
		data["requestId"] = "req-1"
		data["ipAddress"] = "2001:db8::1"
		data["userAgent"] = "Mozilla/5.0"
	})
	e, err := domain.ParseEntry("helpdesk", full)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != validID || e.Service != "helpdesk" || e.Action != "helpdesk:ticket:write" || e.Result != "success" ||
		e.Actor != "usr_1" || e.ActorType != "user" || e.Act != "agt_1" || e.ActType != "agent" ||
		e.TargetType != "ticket" || e.TargetID != "t_1" || e.Summary != "Ticket changed" ||
		string(e.Details) != `{"new":"b","old":"a"}` || e.RequestID != "req-1" ||
		e.TraceID != "0af7651916cd43dd8448eb211c80319c" || e.IPAddress != "2001:db8::1" || e.UserAgent != "Mozilla/5.0" ||
		e.Time.Format("2006-01-02T15:04:05Z") != "2026-11-27T10:15:00Z" {
		t.Fatalf("entry = %+v", e)
	}

	e, err = domain.ParseEntry("helpdesk", event(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if e.Act != "" || e.TargetType != "" || e.Summary != "" || e.Details != nil || e.TraceID != "" || e.IPAddress != "" {
		t.Fatalf("minimal entry = %+v", e)
	}
}

// U2: each bad rule gives its reason code.
func TestU2_BadRules(t *testing.T) {
	big := strings.Repeat("a", 16<<10+1)
	cases := []struct {
		name   string
		app    string
		change func(env, data map[string]any)
		raw    []byte
		want   domain.Reason
	}{
		{name: "too large", raw: []byte(`{"x":"` + strings.Repeat("a", domain.MaxEntryBytes) + `"}`), want: domain.ReasonTooLarge},
		{name: "not json", raw: []byte(`{`), want: domain.ReasonInvalidJSON},
		{name: "data is not an object", raw: []byte(`{"data":5}`), want: domain.ReasonInvalidJSON},
		{name: "specversion", change: func(env, _ map[string]any) { env["specversion"] = "0.3" }, want: domain.ReasonInvalidSpecVersion},
		{name: "source of another app", app: "helpdesk", change: func(env, _ map[string]any) { env["source"] = "/crm" }, want: domain.ReasonInvalidSource},
		{name: "type", change: func(env, _ map[string]any) { env["type"] = "helpdesk.audit.other" }, want: domain.ReasonInvalidType},
		{name: "id not a UUID", change: func(env, _ map[string]any) { env["id"] = "e1" }, want: domain.ReasonInvalidID},
		{name: "id in upper case", change: func(env, _ map[string]any) { env["id"] = strings.ToUpper(validID) }, want: domain.ReasonInvalidID},
		{name: "no time", change: func(env, _ map[string]any) { delete(env, "time") }, want: domain.ReasonInvalidTime},
		{name: "bad time", change: func(env, _ map[string]any) { env["time"] = "yesterday" }, want: domain.ReasonInvalidTime},
		{name: "no actor", change: func(env, _ map[string]any) { delete(env, "actor") }, want: domain.ReasonInvalidActor},
		{name: "actor type", change: func(env, _ map[string]any) { env["actortype"] = "robot" }, want: domain.ReasonInvalidActor},
		{name: "act without acttype", change: func(env, _ map[string]any) { env["act"] = "agt_1" }, want: domain.ReasonInvalidAct},
		{name: "acttype without act", change: func(env, _ map[string]any) { env["acttype"] = "agent" }, want: domain.ReasonInvalidAct},
		{name: "traceparent", change: func(env, _ map[string]any) { env["traceparent"] = "00-xyz" }, want: domain.ReasonInvalidTraceparent},
		{name: "action of another app", change: func(_, d map[string]any) { d["action"] = "crm:x:y" }, want: domain.ReasonInvalidAction},
		{name: "action pattern", change: func(_, d map[string]any) { d["action"] = "helpdesk:ticket" }, want: domain.ReasonInvalidAction},
		{name: "no action", change: func(_, d map[string]any) { delete(d, "action") }, want: domain.ReasonInvalidAction},
		{name: "result", change: func(_, d map[string]any) { d["result"] = "ok" }, want: domain.ReasonInvalidResult},
		{name: "target without id", change: func(_, d map[string]any) { d["target"] = map[string]any{"type": "ticket"} }, want: domain.ReasonInvalidTarget},
		{name: "target type", change: func(_, d map[string]any) { d["target"] = map[string]any{"type": "Ticket", "id": "1"} }, want: domain.ReasonInvalidTarget},
		{name: "summary too long", change: func(_, d map[string]any) { d["summary"] = strings.Repeat("a", 1025) }, want: domain.ReasonInvalidSummary},
		{name: "summary empty", change: func(_, d map[string]any) { d["summary"] = "" }, want: domain.ReasonInvalidSummary},
		{name: "details array", change: func(_, d map[string]any) { d["details"] = []any{1} }, want: domain.ReasonInvalidDetails},
		{name: "details too large", change: func(_, d map[string]any) { d["details"] = map[string]any{"k": big} }, want: domain.ReasonInvalidDetails},
		{name: "request id too long", change: func(_, d map[string]any) { d["requestId"] = strings.Repeat("a", 129) }, want: domain.ReasonInvalidRequestID},
		{name: "ip address", change: func(_, d map[string]any) { d["ipAddress"] = "999.1.1.1" }, want: domain.ReasonInvalidIPAddress},
		{name: "ip address with zone", change: func(_, d map[string]any) { d["ipAddress"] = "fe80::1%eth0" }, want: domain.ReasonInvalidIPAddress},
		{name: "ip address with port", change: func(_, d map[string]any) { d["ipAddress"] = "1.2.3.4:80" }, want: domain.ReasonInvalidIPAddress},
		{name: "ip address of 46 characters", change: func(_, d map[string]any) { d["ipAddress"] = strings.Repeat("1", 46) }, want: domain.ReasonInvalidIPAddress},
		{name: "user agent too long", change: func(_, d map[string]any) { d["userAgent"] = strings.Repeat("a", 513) }, want: domain.ReasonInvalidUserAgent},
		{name: "NUL in summary", change: func(_, d map[string]any) { d["summary"] = "a\x00b" }, want: domain.ReasonInvalidText},
		{name: "NUL in details value", change: func(_, d map[string]any) { d["details"] = map[string]any{"k": "a\x00b"} }, want: domain.ReasonInvalidText},
		{name: "NUL in details key", change: func(_, d map[string]any) { d["details"] = map[string]any{"a\x00b": 1} }, want: domain.ReasonInvalidText},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.raw
			if payload == nil {
				payload = event(t, tc.change)
			}
			_, err := domain.ParseEntry("helpdesk", payload)
			if got := reasonOf(t, err); got != tc.want {
				t.Fatalf("reason = %s, want %s", got, tc.want)
			}
			if strings.Contains(err.Error(), "ticket") || strings.Contains(err.Error(), "usr_1") {
				t.Fatalf("the error text has a value of the entry: %v", err)
			}
		})
	}

	// A full CloudEvent of exactly 64 KiB passes the size check.
	_, err := domain.ParseEntry("helpdesk", make([]byte, domain.MaxEntryBytes))
	if got := reasonOf(t, err); got == domain.ReasonTooLarge {
		t.Fatal("a payload of exactly the limit is not too large")
	}
}

// U3: an unknown extension attribute and an unknown member of data are ignored.
func TestU3_UnknownMembers(t *testing.T) {
	payload := event(t, func(env, data map[string]any) {
		env["foo"] = "x"
		data["foo"] = map[string]any{"a": 1}
	})
	e, err := domain.ParseEntry("helpdesk", payload)
	if err != nil {
		t.Fatal(err)
	}
	if e.Details != nil || strings.Contains(e.Summary, "foo") {
		t.Fatalf("entry = %+v", e)
	}
}

func TestEscapedNUL(t *testing.T) {
	cases := map[string]bool{
		`{"a":"\u0000"}`:   true,
		`{"a":"\\u0000"}`:  false,
		`{"a":"\\\u0000"}`: true,
		`{"a":"A"}`:        false,
		`{"a":"x"}`:        false,
	}
	for in, want := range cases {
		if got := domain.HasEscapedNUL([]byte(in)); got != want {
			t.Errorf("%s: got %v, want %v", in, got, want)
		}
	}
}

func TestEventID(t *testing.T) {
	if got := domain.EventID([]byte(`{"id":"e1"}`)); got != "e1" {
		t.Fatalf("id = %q", got)
	}
	for _, in := range []string{`{`, `{"id":5}`, `{"id":"` + strings.Repeat("a", 129) + `"}`, `{"id":"a\u0000"}`} {
		if got := domain.EventID([]byte(in)); got != "" {
			t.Fatalf("%s: id = %q, want empty", in, got)
		}
	}
}
