package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

var _ entries.Reader = (*Entries)(nil)

// selectColumns is the list of columns of a read. The order matches scanRow.
const selectColumns = `id, entry_id::text, service, time, received_at, action, result, actor, actor_type,
	act, act_type, target_type, target_id, summary, details, request_id, trace_id, host(ip_address), user_agent`

const order = ` ORDER BY time DESC, id DESC`

// Limits of the read queries.
const (
	// ReadTimeout is the time limit of one list query, one get query, one
	// count query of an export, and one batch of an export. It includes the
	// wait for a free read slot.
	ReadTimeout = 15 * time.Second
	// MaxParallelReads is the number of list and get queries that run at the
	// same time.
	MaxParallelReads = 4
)

// beginRead waits for a free read slot, and returns a context with the time
// limit. The caller must call done. A wait that is longer than the limit gives
// entries.ErrUnavailable.
func (r *Entries) beginRead(ctx context.Context, op string) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	select {
	case r.reads <- struct{}{}:
		return ctx, func() { <-r.reads; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, fmt.Errorf("%s: %w", op, entries.ErrUnavailable)
	}
}

// where builds the WHERE clause of the filters and the cursor. It returns the
// clause (with a leading space, or empty) and the arguments.
func where(f domain.Filters, after *domain.Cursor) (string, []any) {
	var conds []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if !f.From.IsZero() {
		add("time >= ?", f.From)
	}
	if !f.To.IsZero() {
		add("time < ?", f.To)
	}
	for _, c := range []struct{ col, value string }{
		{"service", f.Service}, {"action", f.Action}, {"result", f.Result}, {"actor", f.Actor},
		{"actor_type", f.ActorType}, {"act", f.Act}, {"target_type", f.TargetType}, {"target_id", f.TargetID},
		{"request_id", f.RequestID}, {"trace_id", f.TraceID},
	} {
		if c.value != "" {
			add(c.col+" = ?", c.value)
		}
	}
	if after != nil {
		args = append(args, after.Time, after.ID)
		conds = append(conds, fmt.Sprintf("(time, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

func text(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func scanRow(row pgx.Row) (domain.StoredEntry, error) {
	var (
		e                                                             domain.StoredEntry
		act, actType, targetType, targetID, summary, requestID, trace *string
		ip, ua                                                        *string
		details                                                       []byte
		seq                                                           int64
		tm, received                                                  time.Time
	)
	err := row.Scan(&seq, &e.ID, &e.Service, &tm, &received, &e.Action, &e.Result, &e.Actor, &e.ActorType,
		&act, &actType, &targetType, &targetID, &summary, &details, &requestID, &trace, &ip, &ua)
	if err != nil {
		return domain.StoredEntry{}, err
	}
	e.Seq, e.Time, e.ReceivedAt = seq, tm.UTC(), received.UTC()
	e.Act, e.ActType, e.TargetType, e.TargetID = text(act), text(actType), text(targetType), text(targetID)
	e.Summary, e.RequestID, e.TraceID, e.IPAddress, e.UserAgent = text(summary), text(requestID), text(trace), text(ip), text(ua)
	e.Details = details
	return e, nil
}

func collect(op string, rows pgx.Rows) ([]domain.StoredEntry, error) {
	defer rows.Close()
	var out []domain.StoredEntry
	for rows.Next() {
		e, err := scanRow(rows)
		if err != nil {
			return nil, wrap(op, err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(op, err)
	}
	return out, nil
}

// Search returns a page of entries. The query uses the keyset (time, id).
func (r *Entries) Search(ctx context.Context, f domain.Filters, after *domain.Cursor, limit int) ([]domain.StoredEntry, error) {
	ctx, done, err := r.beginRead(ctx, "search audit entries")
	if err != nil {
		return nil, err
	}
	defer done()
	clause, args := where(f, after)
	args = append(args, limit)
	rows, err := r.pool.Query(ctx, `SELECT `+selectColumns+` FROM audit_entries`+clause+order+` LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, wrap("search audit entries", err)
	}
	return collect("search audit entries", rows)
}

// Get returns one entry. With a service, the query uses the unique key.
// Without a service, it returns the entry with the oldest receipt time, then
// the smallest internal key.
func (r *Entries) Get(ctx context.Context, id, service string) (domain.StoredEntry, error) {
	ctx, done, err := r.beginRead(ctx, "get audit entry")
	if err != nil {
		return domain.StoredEntry{}, err
	}
	defer done()
	q := `SELECT ` + selectColumns + ` FROM audit_entries WHERE entry_id = $1::uuid`
	args := []any{id}
	if service != "" {
		q += ` AND service = $2`
		args = append(args, service)
	}
	q += ` ORDER BY received_at, id LIMIT 1`
	e, err := scanRow(r.pool.QueryRow(ctx, q, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredEntry{}, entries.ErrNotFound
	}
	if err != nil {
		return domain.StoredEntry{}, wrap("get audit entry", err)
	}
	return e, nil
}

// exportCursor is the name of the server-side cursor of an export.
const exportCursor = "audit_export"

type exportSession struct {
	tx       pgx.Tx
	f        domain.Filters
	declared bool
}

// BeginExport starts a REPEATABLE READ READ ONLY transaction. The snapshot
// starts at the first query, which is the count.
func (r *Entries) BeginExport(ctx context.Context, f domain.Filters) (entries.ExportSession, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, wrap("begin export", err)
	}
	return &exportSession{tx: tx, f: f}, nil
}

func (s *exportSession) Count(ctx context.Context, limit int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()
	clause, args := where(s.f, nil)
	args = append(args, limit)
	var n int
	err := s.tx.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM audit_entries`+clause+` LIMIT $`+strconv.Itoa(len(args))+`) t`, args...).Scan(&n)
	if err != nil {
		return 0, wrap("count export", err)
	}
	return n, nil
}

func (s *exportSession) Next(ctx context.Context, n int) ([]domain.StoredEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()
	if !s.declared {
		clause, args := where(s.f, nil)
		_, err := s.tx.Exec(ctx, `DECLARE `+exportCursor+` NO SCROLL CURSOR FOR SELECT `+selectColumns+` FROM audit_entries`+clause+order, args...)
		if err != nil {
			return nil, wrap("declare export cursor", err)
		}
		s.declared = true
	}
	rows, err := s.tx.Query(ctx, `FETCH FORWARD `+strconv.Itoa(n)+` FROM `+exportCursor)
	if err != nil {
		return nil, wrap("fetch export", err)
	}
	return collect("fetch export", rows)
}

func (s *exportSession) Close(ctx context.Context) {
	// The context can be done already. The rollback must run anyway.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = s.tx.Rollback(rctx)
}
