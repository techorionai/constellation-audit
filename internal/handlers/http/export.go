package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/app/problems"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/handlers/http/gen"
)

// csvHeader is the row of column names of the contract, in order.
var csvHeader = []string{
	"id", "service", "time", "receivedAt", "action", "result", "actor", "actorType", "act", "actType",
	"targetType", "targetId", "summary", "requestId", "traceId", "ipAddress", "userAgent", "details",
}

// csvCell writes one cell. Each cell is in double quotes, and a double quote
// becomes two. A cell that starts with =, +, -, @, a tab, a carriage return, or
// a line feed gets a single quote in front, so that a spreadsheet does not run
// it as a formula.
func csvCell(sb *strings.Builder, v string) {
	sb.WriteByte('"')
	if v != "" {
		switch v[0] {
		case '=', '+', '-', '@', '\t', '\r', '\n':
			sb.WriteByte('\'')
		}
	}
	for i := 0; i < len(v); i++ {
		if v[i] == '"' {
			sb.WriteByte('"')
		}
		sb.WriteByte(v[i])
	}
	sb.WriteByte('"')
}

// csvRow writes a row with the line end of RFC 4180.
func csvRow(w io.Writer, cells []string) error {
	var sb strings.Builder
	for i, c := range cells {
		if i > 0 {
			sb.WriteByte(',')
		}
		csvCell(&sb, c)
	}
	sb.WriteString("\r\n")
	_, err := io.WriteString(w, sb.String())
	return err
}

func csvCells(e domain.StoredEntry) []string {
	return []string{
		e.ID, e.Service, e.Time.UTC().Format(time.RFC3339Nano), e.ReceivedAt.UTC().Format(time.RFC3339Nano),
		e.Action, e.Result, e.Actor, e.ActorType, e.Act, e.ActType, e.TargetType, e.TargetID, e.Summary,
		e.RequestID, e.TraceID, e.IPAddress, e.UserAgent, string(e.Details),
	}
}

// csvWriter writes the CSV body.
type csvWriter struct{ w io.Writer }

func (c csvWriter) start() error { return csvRow(c.w, csvHeader) }

func (c csvWriter) entry(_ int, e domain.StoredEntry) error { return csvRow(c.w, csvCells(e)) }

func (c csvWriter) end() error { return nil }

// jsonWriter writes the body as one JSON array.
type jsonWriter struct{ w io.Writer }

func (j jsonWriter) start() error {
	_, err := io.WriteString(j.w, "[")
	return err
}

func (j jsonWriter) entry(n int, e domain.StoredEntry) error {
	b, err := json.Marshal(toAPI(e))
	if err != nil {
		return err
	}
	if n > 0 {
		if _, err := io.WriteString(j.w, ","); err != nil {
			return err
		}
	}
	_, err = j.w.Write(b)
	return err
}

func (j jsonWriter) end() error {
	_, err := io.WriteString(j.w, "]")
	return err
}

type bodyWriter interface {
	start() error
	entry(n int, e domain.StoredEntry) error
	end() error
}

// exportResponse is the response of an export. It streams the body.
type exportResponse struct {
	ctx    context.Context //nolint:containedctx // The strict handler gives the context at the call of the operation only.
	s      *Server
	stream *commands.ExportStream
	format gen.AuditExportFormat
}

// VisitAuditEntriesExportResponse writes the headers and the body. The first
// batch is read before the headers, so a store error at the start gives a
// Problem. An error after the headers stops the response: the handler logs it
// and aborts the connection, so the client sees an incomplete body (decision
// D8).
func (r *exportResponse) VisitAuditEntriesExportResponse(w http.ResponseWriter) error {
	first, err := r.stream.Next(r.ctx)
	if err != nil {
		r.stream.Finish(r.ctx, err)
		return problems.FromStore(err)
	}
	// A large export takes longer than the write timeout of the server.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(r.s.d.ExportTimeout))

	ext, contentType := "csv", "text/csv"
	if r.format == gen.Json {
		ext, contentType = "json", "application/json"
	}
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Disposition", `attachment; filename="audit-entries-`+time.Now().UTC().Format("20060102T150405Z")+"."+ext+`"`)
	h.Set("Cache-Control", cacheControl)
	h.Set("Pragma", pragma)
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriterSize(w, 32<<10)
	var body bodyWriter = csvWriter{bw}
	if r.format == gen.Json {
		body = jsonWriter{bw}
	}
	err = r.write(bw, body, first)
	r.stream.Finish(r.ctx, err)
	if err != nil {
		r.s.log.ErrorContext(r.ctx, "an export stopped after the first byte", "error", err)
		panic(http.ErrAbortHandler)
	}
	return nil
}

func (r *exportResponse) write(bw *bufio.Writer, body bodyWriter, batch []domain.StoredEntry) error {
	if err := body.start(); err != nil {
		return err
	}
	n := 0
	for len(batch) > 0 {
		for _, e := range batch {
			if err := body.entry(n, e); err != nil {
				return err
			}
			n++
		}
		var err error
		if batch, err = r.stream.Next(r.ctx); err != nil {
			return err
		}
	}
	if err := body.end(); err != nil {
		return err
	}
	return bw.Flush()
}
