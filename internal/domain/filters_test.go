package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-audit/internal/domain"
)

// U1: filter checks. Each bad case gives a FilterError that names the
// parameter. The two values at the limit are valid.
func TestU1_FilterChecks(t *testing.T) {
	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	bad := []struct {
		name  string
		f     domain.Filters
		param string
	}{
		{"targetId without targetType", domain.Filters{TargetID: "t1"}, "targetId"},
		{"from equal to to", domain.Filters{From: now, To: now}, "from"},
		{"from after to", domain.Filters{From: now.Add(time.Hour), To: now}, "from"},
		{"service Bad-App", domain.Filters{Service: "Bad-App"}, "service"},
		{"service of 33 characters", domain.Filters{Service: "a" + strings.Repeat("b", 32)}, "service"},
		{"action with a bad form", domain.Filters{Action: "helpdesk"}, "action"},
		{"traceId in upper case", domain.Filters{TraceID: strings.Repeat("A", 32)}, "traceId"},
		{"traceId of 31 characters", domain.Filters{TraceID: strings.Repeat("a", 31)}, "traceId"},
		{"result ok", domain.Filters{Result: "ok"}, "result"},
		{"actorType robot", domain.Filters{ActorType: "robot"}, "actorType"},
		{"targetType in upper case", domain.Filters{TargetType: "Ticket"}, "targetType"},
		{"actor of 129 characters", domain.Filters{Actor: strings.Repeat("a", 129)}, "actor"},
		{"act of 129 characters", domain.Filters{Act: strings.Repeat("a", 129)}, "act"},
		{"targetId of 257 characters", domain.Filters{TargetType: "ticket", TargetID: strings.Repeat("a", 257)}, "targetId"},
		{"requestId of 129 characters", domain.Filters{RequestID: strings.Repeat("a", 129)}, "requestId"},
		{"actor with U+0000", domain.Filters{Actor: "a\x00b"}, "actor"},
		{"targetId with the byte 0xFF", domain.Filters{TargetType: "ticket", TargetID: "a\xffb"}, "targetId"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.f.Validate()
			var fe *domain.FilterError
			if !errors.As(err, &fe) || fe.Param != tc.param {
				t.Fatalf("error = %v, want a FilterError for %s", err, tc.param)
			}
			if strings.IndexByte(err.Error(), 0) >= 0 || strings.Contains(err.Error(), "\xff") {
				t.Fatalf("the error text has a value: %q", err.Error())
			}
		})
	}
	good := []domain.Filters{
		{},
		{Actor: strings.Repeat("a", 128)},
		{TargetType: "ticket", TargetID: strings.Repeat("é", 256)},
		{From: now, To: now.Add(time.Nanosecond), Service: "helpdesk", Action: "helpdesk:ticket:write", Result: "denied",
			ActorType: "user", Act: "agent_1", RequestID: "req-1", TraceID: strings.Repeat("a", 32)},
	}
	for i, f := range good {
		if err := f.Validate(); err != nil {
			t.Fatalf("good case %d: %v", i, err)
		}
	}
}

// U2: the same filters give the same hash. Another value gives another hash.
func TestU2_FilterHash(t *testing.T) {
	a := domain.Filters{Service: "helpdesk", Actor: "usr_1", Result: "success"}
	b := domain.Filters{Result: "success", Actor: "usr_1", Service: "helpdesk"}
	if a.Hash() != b.Hash() {
		t.Fatal("the same filters must give the same hash")
	}
	c := domain.Filters{Service: "helpdesk", Actor: "usr_2", Result: "success"}
	if a.Hash() == c.Hash() {
		t.Fatal("other filters must give another hash")
	}
	// The same instant in another zone gives the same hash.
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.In(time.FixedZone("x", 3600))
	if (domain.Filters{From: t1}).Hash() != (domain.Filters{From: t2}).Hash() {
		t.Fatal("the same instant must give the same hash")
	}
	if (domain.Filters{}).Hash() == a.Hash() {
		t.Fatal("empty filters must give another hash")
	}
}
