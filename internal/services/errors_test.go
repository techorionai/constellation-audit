package services

import (
	"errors"
	"log/slog"
	"testing"

	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
)

// R6: an error that the caller caused is logged at INFO and is not a failure
// of the span.
func TestReadErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err    error
		level  slog.Level
		client bool
	}{
		{apperrors.NewBadRequest("invalid_parameter", "x"), slog.LevelInfo, true},
		{apperrors.NewNotFound("audit_entry_not_found", "x"), slog.LevelInfo, true},
		{apperrors.NewUnprocessable("export_too_large", "x"), slog.LevelInfo, true},
		{apperrors.NewTooManyRequests("too_many_exports", "x", 0), slog.LevelInfo, true},
		{apperrors.NewUnavailable(), slog.LevelError, false},
		{errors.New("other"), slog.LevelError, false},
	} {
		if got := readErrorLevel(tc.err); got != tc.level {
			t.Errorf("%v: level = %v, want %v", tc.err, got, tc.level)
		}
		if got := !readErrorFailure(tc.err); got != tc.client {
			t.Errorf("%v: client = %v, want %v", tc.err, got, tc.client)
		}
	}
}
