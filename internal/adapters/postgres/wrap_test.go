package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// TestWrapUnavailableCodes: the error classes 08 and 53 and the codes 57P01 to
// 57P03 mean that the store is not available. The class 22 means that the store
// refused the data. Another code is a failure of the service.
func TestWrapUnavailableCodes(t *testing.T) {
	cases := map[string]struct{ unavailable, refused bool }{
		"08000": {true, false}, "08006": {true, false}, "08001": {true, false},
		"53000": {true, false}, "53200": {true, false}, "53300": {true, false},
		"57P01": {true, false}, "57P02": {true, false}, "57P03": {true, false},
		"22P05": {false, true}, "22021": {false, true}, "22P02": {false, true}, "22000": {false, true},
		"23505": {false, false}, "42P01": {false, false}, "57014": {false, false}, "23000": {false, false},
	}
	for code, want := range cases {
		err := wrap("op", &pgconn.PgError{Code: code, Message: "invalid value SECRET-VALUE"})
		if got := errors.Is(err, entries.ErrUnavailable); got != want.unavailable {
			t.Errorf("code %s: unavailable = %v, want %v", code, got, want.unavailable)
		}
		if got := errors.Is(err, entries.ErrDataRefused); got != want.refused {
			t.Errorf("code %s: refused = %v, want %v", code, got, want.refused)
		}
		if strings.Contains(err.Error(), "SECRET-VALUE") {
			t.Errorf("code %s: the error text has the message of the database", code)
		}
	}
	for name, in := range map[string]error{
		"closed":      io.EOF,
		"unexpected":  io.ErrUnexpectedEOF,
		"timeout":     context.DeadlineExceeded,
		"net":         &net.OpError{Op: "read", Err: errors.New("reset")},
		"closed pool": puddle.ErrClosedPool,
	} {
		if err := wrap("op", in); !errors.Is(err, entries.ErrUnavailable) {
			t.Errorf("%s: a connection error must be unavailable", name)
		}
	}
	// R7: an error with no code that is not a connection error is a failure of
	// the service. Its text stays out of the result.
	err := wrap("op", errors.New("unable to encode SECRET-VALUE"))
	if errors.Is(err, entries.ErrUnavailable) || errors.Is(err, entries.ErrDataRefused) || strings.Contains(err.Error(), "SECRET-VALUE") {
		t.Errorf("a client side error must be a plain failure with no value: %v", err)
	}
	if wrap("op", nil) != nil {
		t.Error("nil must stay nil")
	}
}
