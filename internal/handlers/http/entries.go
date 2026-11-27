package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/techorionai/constellation-platform-go/auth"
	"github.com/techorionai/constellation-platform-go/clientinfo"
	"github.com/techorionai/constellation-platform-go/requestid"
	"go.opentelemetry.io/otel/trace"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/app/problems"
	"github.com/techorionai/constellation-audit/internal/app/queries"
	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/handlers/http/gen"
)

const (
	// maxTokenLength is the largest page token that the service decodes.
	maxTokenLength = 1024
	tokenOpEntries = "entries"
	// cacheControl and pragma are the cache headers of all responses of the
	// operations.
	cacheControl = "no-store"
	pragma       = "no-cache"
)

var errInvalidToken = apperrors.NewBadRequest("invalid_page_token", "The page token is not valid.")

// pageToken is the content of a page token. It has no secret. The service
// checks each value, so it needs no signature. H binds the token to the
// filters.
type pageToken struct {
	Op string `json:"o"`
	H  string `json:"h"`
	T  int64  `json:"t"`
	I  int64  `json:"i"`
}

// EncodePageToken makes a page token from the filters and the cursor.
func EncodePageToken(f domain.Filters, c domain.Cursor) string {
	b, _ := json.Marshal(pageToken{Op: tokenOpEntries, H: f.Hash(), T: c.Time.UnixMicro(), I: c.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodePageToken returns the cursor of a page token. The token must be for the
// operation and the filters. An empty token gives a nil cursor.
func DecodePageToken(token string, f domain.Filters) (*domain.Cursor, error) {
	if token == "" {
		return nil, nil
	}
	if len(token) > maxTokenLength {
		return nil, errInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, errInvalidToken
	}
	var t pageToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, errInvalidToken
	}
	if t.Op != tokenOpEntries || t.H != f.Hash() || t.I < 1 {
		return nil, errInvalidToken
	}
	return &domain.Cursor{Time: time.UnixMicro(t.T).UTC(), ID: t.I}, nil
}

// pageSize checks the pageSize parameter. A missing value gives the default.
func pageSize(v *int32) (int, error) {
	if v == nil {
		return queries.DefaultPageSize, nil
	}
	if int(*v) < 1 || int(*v) > queries.MaxPageSize {
		return 0, problems.InvalidParameter("pageSize")
	}
	return int(*v), nil
}

// rawFilters holds the filter parameters of a request. The list and the
// export have the same parameters.
type rawFilters struct {
	From, To                                       *time.Time
	Service, Action, Result, Actor, ActorType, Act *string
	TargetType, TargetID, RequestID, TraceID       *string
}

// checkSet rejects a filter that the request sets with an empty value. The
// domain cannot see the difference between an empty value and a missing value,
// so the handler does this check.
func (r rawFilters) checkSet() error {
	for _, c := range []struct {
		param string
		value *string
	}{
		{"service", r.Service}, {"action", r.Action}, {"result", r.Result}, {"actor", r.Actor},
		{"actorType", r.ActorType}, {"act", r.Act}, {"targetType", r.TargetType}, {"targetId", r.TargetID},
		{"requestId", r.RequestID}, {"traceId", r.TraceID},
	} {
		if c.value != nil && *c.value == "" {
			return problems.InvalidParameter(c.param)
		}
	}
	return nil
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func tm(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.UTC()
}

func (r rawFilters) filters() domain.Filters {
	return domain.Filters{
		From: tm(r.From), To: tm(r.To), Service: str(r.Service), Action: str(r.Action), Result: str(r.Result),
		Actor: str(r.Actor), ActorType: str(r.ActorType), Act: str(r.Act), TargetType: str(r.TargetType),
		TargetID: str(r.TargetID), RequestID: str(r.RequestID), TraceID: str(r.TraceID),
	}
}

func strOf[T ~string](p *T) *string {
	if p == nil {
		return nil
	}
	s := string(*p)
	return &s
}

func listFilters(p gen.AuditEntriesListParams) rawFilters {
	return rawFilters{
		From: p.From, To: p.To, Service: strOf(p.Service), Action: strOf(p.Action), Result: strOf(p.Result),
		Actor: strOf(p.Actor), ActorType: strOf(p.ActorType), Act: strOf(p.Act), TargetType: strOf(p.TargetType),
		TargetID: strOf(p.TargetId), RequestID: strOf(p.RequestId), TraceID: strOf(p.TraceId),
	}
}

func exportFilters(p gen.AuditEntriesExportParams) rawFilters {
	return rawFilters{
		From: p.From, To: p.To, Service: strOf(p.Service), Action: strOf(p.Action), Result: strOf(p.Result),
		Actor: strOf(p.Actor), ActorType: strOf(p.ActorType), Act: strOf(p.Act), TargetType: strOf(p.TargetType),
		TargetID: strOf(p.TargetId), RequestID: strOf(p.RequestId), TraceID: strOf(p.TraceId),
	}
}

// toAPI turns an entry into the type of the contract. The details keep the
// exact numbers of the stored JSON.
func toAPI(e domain.StoredEntry) gen.AuditEntry {
	id, _ := uuid.Parse(e.ID) // The store keeps a UUID.
	out := gen.AuditEntry{
		Id: id, Service: e.Service, Time: e.Time, ReceivedAt: e.ReceivedAt, Action: e.Action,
		Result: gen.AuditResult(e.Result), Actor: e.Actor, ActorType: gen.PrincipalType(e.ActorType),
	}
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	if e.Act != "" {
		out.Act = opt(e.Act)
		t := gen.PrincipalType(e.ActType)
		out.ActType = &t
	}
	if e.TargetType != "" {
		out.Target = &gen.AuditTarget{Type: e.TargetType, Id: e.TargetID}
	}
	out.Summary, out.RequestId, out.TraceId = opt(e.Summary), opt(e.RequestID), opt(e.TraceID)
	out.IpAddress, out.UserAgent = opt(e.IPAddress), opt(e.UserAgent)
	if len(e.Details) > 0 {
		dec := json.NewDecoder(bytes.NewReader(e.Details))
		dec.UseNumber()
		var m map[string]any
		if dec.Decode(&m) == nil {
			out.Details = &m
		}
	}
	return out
}

// AuditEntriesList searches the entries.
func (s *Server) AuditEntriesList(ctx context.Context, req gen.AuditEntriesListRequestObject) (gen.AuditEntriesListResponseObject, error) {
	size, err := pageSize(req.Params.PageSize)
	if err != nil {
		return nil, err
	}
	raw := listFilters(req.Params)
	if err := raw.checkSet(); err != nil {
		return nil, err
	}
	f := raw.filters()
	if err := f.Validate(); err != nil {
		return nil, problems.FromFilter(err)
	}
	after, err := DecodePageToken(str(req.Params.PageToken), f)
	if err != nil {
		return nil, err
	}
	res, err := s.d.List.Execute(ctx, queries.ListEntries{Filters: f, After: after, PageSize: size})
	if err != nil {
		return nil, err
	}
	page := gen.AuditEntryPage{Items: make([]gen.AuditEntry, 0, len(res.Items))}
	for _, e := range res.Items {
		page.Items = append(page.Items, toAPI(e))
	}
	if res.Next != nil {
		t := EncodePageToken(f, *res.Next)
		page.NextPageToken = &t
	}
	return gen.AuditEntriesList200JSONResponse{Body: page, Headers: gen.AuditEntriesList200ResponseHeaders{CacheControl: cacheControl, Pragma: pragma}}, nil
}

// AuditEntriesGet returns one entry.
func (s *Server) AuditEntriesGet(ctx context.Context, req gen.AuditEntriesGetRequestObject) (gen.AuditEntriesGetResponseObject, error) {
	e, err := s.d.Get.Execute(ctx, queries.GetEntry{ID: req.Id.String(), Service: str(req.Params.Service)})
	if err != nil {
		return nil, err
	}
	return gen.AuditEntriesGet200JSONResponse{Body: toAPI(e), Headers: gen.AuditEntriesGet200ResponseHeaders{CacheControl: cacheControl, Pragma: pragma}}, nil
}

// requestInfo reads the request data for the own audit entry.
func requestInfo(ctx context.Context) commands.RequestInfo {
	info := commands.RequestInfo{RequestID: requestid.FromContext(ctx)}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		info.TraceID = sc.TraceID().String()
	}
	if ci, ok := clientinfo.FromContext(ctx); ok {
		if ci.IP.IsValid() && ci.IP.Zone() == "" {
			info.IPAddress = ci.IP.String()
		}
		info.UserAgent = ci.UserAgent
	}
	return info
}

// principalOf reads the verified principal. The token check runs before the
// handler, so a missing principal is an error of the service.
func principalOf(ctx context.Context) (commands.Principal, error) {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return commands.Principal{}, errors.New("the request has no principal")
	}
	out := commands.Principal{ID: p.ID, Type: string(p.Type)}
	if p.Act != nil {
		out.Act, out.ActType = p.Act.ID, string(p.Act.Type)
	}
	return out, nil
}

// AuditEntriesExport checks the request, writes the own audit entry, and
// returns a response that streams the body.
func (s *Server) AuditEntriesExport(ctx context.Context, req gen.AuditEntriesExportRequestObject) (gen.AuditEntriesExportResponseObject, error) {
	if !req.Params.Format.Valid() {
		return nil, problems.InvalidParameter("format")
	}
	raw := exportFilters(req.Params)
	if err := raw.checkSet(); err != nil {
		return nil, err
	}
	f := raw.filters()
	if err := f.Validate(); err != nil {
		return nil, problems.FromFilter(err)
	}
	principal, err := principalOf(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := s.d.Export.Execute(ctx, commands.ExportEntries{
		Filters: f, Format: string(req.Params.Format), Principal: principal, Request: requestInfo(ctx),
	})
	if err != nil {
		return nil, err
	}
	return &exportResponse{ctx: ctx, s: s, stream: stream, format: req.Params.Format}, nil
}
