// Package postgres is the PostgreSQL adapter of the audit store.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"

	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// Entries is the PostgreSQL repository of the audit entries and the rejected
// entries.
type Entries struct {
	pool *pgxpool.Pool
}

var _ entries.Repository = (*Entries)(nil)

// New makes the repository.
func New(pool *pgxpool.Pool) *Entries { return &Entries{pool: pool} }

// unavailableCode tells whether a PostgreSQL error code means that the store
// cannot serve requests now. The classes are 08 (connection exception), 53
// (insufficient resources), and 57P (operator intervention: shutdown, crash
// shutdown, cannot connect now).
func unavailableCode(code string) bool {
	return strings.HasPrefix(code, "08") || strings.HasPrefix(code, "53") ||
		code == "57P01" || code == "57P02" || code == "57P03"
}

// connectionError tells whether an error with no PostgreSQL code comes from the
// connection: a network error, a refused connection, a timeout, a closed
// connection, or a closed pool. Other errors with no code, for example an
// encode error of a value on the client side, come from the data or from the
// service, so a retry cannot help.
func connectionError(err error) bool {
	var netErr net.Error
	var connErr *pgconn.ConnectError
	return errors.As(err, &netErr) || errors.As(err, &connErr) || pgconn.Timeout(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, puddle.ErrClosedPool) || errors.Is(err, puddle.ErrNotAvailable)
}

// wrap maps an error of the driver. A connection error with no PostgreSQL code
// (see connectionError) and an error with a code of
// class 08, class 53, or 57P01 to 57P03 is an unavailable store. An error of
// class 22 (data exception) comes from the data, so it is a refusal of the
// data. Each other error with a code is a failure of the service.
//
// The text of an error with a code never goes into the result. The message of
// a PostgreSQL data error can hold a value of the entry, and a value must not
// go into a log or a span.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		if connectionError(err) {
			return fmt.Errorf("%s: %w: %w", op, entries.ErrUnavailable, err)
		}
		// The text of this error can hold a value of the entry, so it stays out.
		return fmt.Errorf("%s: driver error of type %T", op, err)
	}
	switch {
	case unavailableCode(pgErr.Code):
		return fmt.Errorf("%s: %w: PostgreSQL code %s", op, entries.ErrUnavailable, pgErr.Code)
	case strings.HasPrefix(pgErr.Code, "22"):
		return fmt.Errorf("%s: %w: PostgreSQL code %s", op, entries.ErrDataRefused, pgErr.Code)
	default:
		return fmt.Errorf("%s: PostgreSQL code %s", op, pgErr.Code)
	}
}

// Insert stores an entry. The unique key (service, entry_id) makes a second
// delivery of the same entry a no-op.
func (r *Entries) Insert(ctx context.Context, e domain.Entry) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO audit_entries (entry_id, service, time, action, result, actor, actor_type, act, act_type,
			target_type, target_id, summary, details, request_id, trace_id, ip_address, user_agent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::text::inet, $17)
		ON CONFLICT (service, entry_id) DO NOTHING`,
		e.ID, e.Service, e.Time, e.Action, e.Result, e.Actor, e.ActorType,
		nullable(e.Act), nullable(e.ActType), nullable(e.TargetType), nullable(e.TargetID), nullable(e.Summary),
		e.Details, nullable(e.RequestID), nullable(e.TraceID), nullable(e.IPAddress), nullable(e.UserAgent))
	if err != nil {
		return false, wrap("insert audit entry", err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertRejected stores a rejected entry. A second delivery of the same message
// changes nothing.
func (r *Entries) InsertRejected(ctx context.Context, rej domain.Rejected) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO rejected_entries (stream, stream_id, entry_id, reason, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (stream, stream_id) DO NOTHING`,
		rej.Stream, rej.StreamID, nullable(rej.EntryID), string(rej.Reason), rej.Payload)
	return wrap("insert rejected entry", err)
}

// nullable maps an empty string to NULL.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

var _ entries.KnownApps = (*Entries)(nil)

// KnownApps returns the sorted apps that the intake found, a maximum of limit.
func (r *Entries) KnownApps(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT app FROM known_apps ORDER BY app LIMIT $1`, limit)
	if err != nil {
		return nil, wrap("list known apps", err)
	}
	defer rows.Close()
	var apps []string
	for rows.Next() {
		var app string
		if err := rows.Scan(&app); err != nil {
			return nil, wrap("scan known app", err)
		}
		apps = append(apps, app)
	}
	return apps, wrap("list known apps", rows.Err())
}

// RememberApp keeps an app. A second call with the same app changes nothing.
func (r *Entries) RememberApp(ctx context.Context, app string) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO known_apps (app) VALUES ($1) ON CONFLICT (app) DO NOTHING`, app)
	return wrap("remember app", err)
}
