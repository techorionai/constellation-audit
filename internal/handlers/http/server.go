// Package httpapi holds the HTTP handlers of the audit service. They are the
// primary adapters. They call internal/app only.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/techorionai/constellation-platform-go/auth"
	"github.com/techorionai/constellation-platform-go/requestid"
	"go.opentelemetry.io/otel/trace"

	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/handlers/http/gen"
)

// CodeNotImplemented is the code of the Problem of an operation that the service
// does not implement yet.
const CodeNotImplemented = "not_implemented"

// Server implements the generated strict server interface.
type Server struct {
	d      Deps
	log    *slog.Logger
	authMW func(http.Handler) http.Handler
}

var _ gen.StrictServerInterface = (*Server)(nil)

// Deps are the token check of /v1 and the logger.
type Deps struct {
	// Verifier checks the bearer token of each /v1 request. Scopes gives the
	// scopes of each /v1 operation. Without them, each /v1 request fails with
	// a 500 Problem, because the service fails closed.
	Verifier auth.Verifier
	Scopes   ScopeTable
	Log      *slog.Logger
}

// New makes the server.
func New(d Deps) *Server {
	log := d.Log
	if log == nil {
		log = slog.Default()
	}
	s := &Server{d: d, log: log}
	if d.Verifier != nil {
		s.authMW = auth.Middleware(d.Verifier)
	}
	return s
}

// Handler returns the router of all operations of the contract, with the
// error handlers that write Problem bodies. The chain of each /v1 route is
// auth.Middleware, then auth.RequireScopes with the scopes of the operation,
// then the handler.
func (s *Server) Handler() http.Handler {
	strict := gen.NewStrictHandlerWithOptions(s, nil, gen.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  s.requestError,
		ResponseErrorHandlerFunc: s.responseError,
	})
	return gen.HandlerWithOptions(strict, gen.StdHTTPServerOptions{
		ErrorHandlerFunc: s.requestError,
		Middlewares:      []gen.MiddlewareFunc{s.protect},
	})
}

// protect runs the token check on the /v1 routes. The route pattern gives the
// scopes. A route with no entry in the scope table is an error of the service,
// and the request fails (fail closed).
func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		scopes, ok := s.d.Scopes[r.Pattern]
		if !ok || s.authMW == nil {
			s.log.ErrorContext(r.Context(), "a /v1 route has no scope table entry or the service has no verifier", "pattern", r.Pattern)
			s.writeProblem(w, r, http.StatusInternalServerError, "auth_not_configured", "Authentication not configured", "The server has an internal error.")
			return
		}
		s.authMW(auth.RequireScopes(scopes...)(next)).ServeHTTP(w, r)
	})
}

// AuditEntriesList is not implemented in this version.
func (s *Server) AuditEntriesList(context.Context, gen.AuditEntriesListRequestObject) (gen.AuditEntriesListResponseObject, error) {
	return nil, notImplemented()
}

// AuditEntriesExport is not implemented in this version.
func (s *Server) AuditEntriesExport(context.Context, gen.AuditEntriesExportRequestObject) (gen.AuditEntriesExportResponseObject, error) {
	return nil, notImplemented()
}

// AuditEntriesGet is not implemented in this version.
func (s *Server) AuditEntriesGet(context.Context, gen.AuditEntriesGetRequestObject) (gen.AuditEntriesGetResponseObject, error) {
	return nil, notImplemented()
}

func notImplemented() error {
	return apperrors.NewProblem(http.StatusNotImplemented, CodeNotImplemented, "The operation is not implemented.")
}

func (s *Server) requestError(w http.ResponseWriter, r *http.Request, _ error) {
	s.writeProblem(w, r, http.StatusBadRequest, "invalid_request", "Invalid request", "The request is not valid.")
}

func (s *Server) responseError(w http.ResponseWriter, r *http.Request, err error) {
	if ve, ok := apperrors.AsValidationError(err); ok {
		s.writeValidation(w, r, ve)
		return
	}
	if pe, ok := apperrors.AsProblem(err); ok {
		if pe.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int((pe.RetryAfter+time.Second-1)/time.Second)))
		}
		s.writeProblem(w, r, pe.Status, pe.Code, http.StatusText(pe.Status), pe.Detail)
		return
	}
	// The error text can hold internal data. It goes to the log only.
	s.log.ErrorContext(r.Context(), "request failed", "error", err)
	s.writeProblem(w, r, http.StatusInternalServerError, "internal_error", "Internal error", "The server has an internal error.")
}

func (s *Server) problem(r *http.Request, status int, code, title, detail string) gen.Problem {
	p := gen.Problem{
		Type:   "about:blank",
		Title:  title,
		Status: int32(status), //nolint:gosec // The status is a small constant.
		Code:   code,
		Detail: &detail,
	}
	if id := requestid.FromContext(r.Context()); id != "" {
		p.RequestId = &id
	}
	if sc := trace.SpanContextFromContext(r.Context()); sc.HasTraceID() {
		t := sc.TraceID().String()
		p.TraceId = &t
	}
	return p
}

func (s *Server) writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string) {
	p := s.problem(r, status, code, title, detail)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

// writeValidation writes a 422 Problem with the field errors. The code of the
// Problem is the code of the first field error, or the code of the error.
func (s *Server) writeValidation(w http.ResponseWriter, r *http.Request, ve *apperrors.ValidationError) {
	p := s.problem(r, http.StatusUnprocessableEntity, ve.Code, "Unprocessable content", "The content of the request is not valid.")
	if len(ve.Fields) > 0 {
		fields := make([]gen.FieldError, 0, len(ve.Fields))
		for _, f := range ve.Fields {
			fields = append(fields, gen.FieldError{Code: f.Code, Field: f.Field, Message: f.Message})
		}
		p.Errors = &fields
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(p)
}
