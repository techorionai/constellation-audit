package httpapi_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/auth"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/techorionai/constellation-audit/api/openapi"
	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/app/queries"
	"github.com/techorionai/constellation-audit/internal/domain"
	httpapi "github.com/techorionai/constellation-audit/internal/handlers/http"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

const entryID = "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88"

// wantReader is the status that a token with the read scope gets from a mock
// store that is empty.
var wantReader = map[string]int{
	"GET /v1/audit-entries":        http.StatusOK,
	"GET /v1/audit-entries/export": http.StatusOK,
	"GET /v1/audit-entries/{id}":   http.StatusNotFound,
}

func newServer(t *testing.T, table httpapi.ScopeTable, v auth.Verifier, reader *mock.Reader, writer *mock.Entries) *httpapi.Server {
	t.Helper()
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	export, err := commands.NewExportEntries(reader, writer, mp.Meter("test"), 100, 2, log)
	if err != nil {
		t.Fatal(err)
	}
	return httpapi.New(httpapi.Deps{
		Verifier: v, Scopes: table, Log: log,
		List: queries.NewListEntries(reader), Get: queries.NewGetEntry(reader), Export: export, ExportTimeout: time.Minute,
	})
}

func readerServer(t *testing.T, reader *mock.Reader, writer *mock.Entries) http.Handler {
	t.Helper()
	table, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		t.Fatal(err)
	}
	v := fakeVerifier{"reader": {ID: "svc_reader", Type: auth.PrincipalService, App: "reader", Scopes: []string{readScope}}}
	return newServer(t, table, v, reader, writer).Handler()
}

func sample(seq int64, summary string) domain.StoredEntry {
	return domain.StoredEntry{
		Seq: seq, ReceivedAt: time.Date(2026, 11, 27, 10, 0, 1, 0, time.UTC),
		Entry: domain.Entry{
			ID: entryID, Service: "helpdesk", Time: time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC), Action: "helpdesk:ticket:write",
			Result: "success", Actor: "usr_1", ActorType: "user", Summary: summary, Details: []byte(`{"n": 12345678901234567890, "a": [1, 2]}`),
		},
	}
}

// U4: page token. Encode and decode, a token of 1025 characters, a token with
// other filters, a token of another operation.
func TestU4_PageToken(t *testing.T) {
	f := domain.Filters{Service: "helpdesk"}
	cur := domain.Cursor{Time: time.Date(2026, 11, 27, 10, 0, 0, 123456000, time.UTC), ID: 42}
	tok := httpapi.EncodePageToken(f, cur)
	got, err := httpapi.DecodePageToken(tok, f)
	if err != nil || got == nil || !got.Time.Equal(cur.Time) || got.ID != 42 {
		t.Fatalf("got = %+v, err = %v", got, err)
	}
	if got, err := httpapi.DecodePageToken("", f); err != nil || got != nil {
		t.Fatalf("empty token: %v, %v", got, err)
	}
	for name, token := range map[string]string{
		"1025 characters": strings.Repeat("a", 1025),
		"other filters":   httpapi.EncodePageToken(domain.Filters{Service: "crm"}, cur),
		"not base64":      "!!!",
		"not JSON":        "e30",
		"other operation": "eyJvIjoidHlwZXMiLCJoIjoiIiwidCI6MSwiaSI6MX0",
		"no cursor id":    httpapi.EncodePageToken(f, domain.Cursor{Time: cur.Time}),
	} {
		_, err := httpapi.DecodePageToken(token, f)
		if err == nil || !strings.Contains(err.Error(), "invalid_page_token") {
			t.Fatalf("%s: error = %v", name, err)
		}
	}
}

func get(h http.Handler, path string) *httptest.ResponseRecorder { return do(h, path, "reader") }

func problemCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body = %s", rec.Body.String())
	}
	return p.Code
}

// U10: the request error handler and pageSize.
func TestU10_RequestErrors(t *testing.T) {
	var gotLimit int
	reader := &mock.Reader{SearchFunc: func(_ context.Context, _ domain.Filters, _ *domain.Cursor, limit int) ([]domain.StoredEntry, error) {
		gotLimit = limit
		return nil, nil
	}}
	h := readerServer(t, reader, &mock.Entries{})
	cases := []struct {
		path   string
		status int
		code   string
	}{
		{"/v1/audit-entries/abc", 404, "audit_entry_not_found"},
		{"/v1/audit-entries/" + strings.ToUpper(entryID), 404, "audit_entry_not_found"},
		{"/v1/audit-entries?from=yesterday", 400, "invalid_parameter"},
		{"/v1/audit-entries/export", 400, "invalid_parameter"},
		{"/v1/audit-entries/export?format=xml", 400, "invalid_parameter"},
		{"/v1/audit-entries?pageSize=0", 400, "invalid_parameter"},
		{"/v1/audit-entries?pageSize=201", 400, "invalid_parameter"},
		{"/v1/audit-entries?pageSize=abc", 400, "invalid_parameter"},
		{"/v1/audit-entries?result=ok", 400, "invalid_parameter"},
		{"/v1/audit-entries?service=a&service=b", 400, "invalid_parameter"},
		{"/v1/audit-entries/" + entryID + "?service=Bad-App", 400, "invalid_parameter"},
		{"/v1/audit-entries?pageToken=zzz", 400, "invalid_page_token"},
		{"/v1/audit-entries?actor=", 400, "invalid_parameter"},
		{"/v1/audit-entries?act=", 400, "invalid_parameter"},
		{"/v1/audit-entries?requestId=", 400, "invalid_parameter"},
		{"/v1/audit-entries?targetId=&targetType=ticket", 400, "invalid_parameter"},
		{"/v1/audit-entries?service=", 400, "invalid_parameter"},
		{"/v1/audit-entries?result=", 400, "invalid_parameter"},
		{"/v1/audit-entries/export?format=csv&actor=", 400, "invalid_parameter"},
		{"/v1/audit-entries?pageSize=1", 200, ""},
		{"/v1/audit-entries?pageSize=200", 200, ""},
	}
	for _, tc := range cases {
		rec := get(h, tc.path)
		if rec.Code != tc.status || (tc.code != "" && problemCode(t, rec) != tc.code) {
			t.Fatalf("%s: status = %d, body = %s", tc.path, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
			t.Fatalf("%s: headers = %v", tc.path, rec.Header())
		}
	}
	get(h, "/v1/audit-entries?pageSize=200")
	if gotLimit != 201 {
		t.Fatalf("limit = %d, want 201", gotLimit)
	}
	get(h, "/v1/audit-entries")
	if gotLimit != 51 {
		t.Fatalf("the default page size gives limit %d, want 51", gotLimit)
	}
}

func TestListPageAndGet(t *testing.T) {
	reader := &mock.Reader{
		SearchFunc: func(_ context.Context, _ domain.Filters, after *domain.Cursor, limit int) ([]domain.StoredEntry, error) {
			out := []domain.StoredEntry{sample(30, "a"), sample(29, "b"), sample(28, "c")}
			return out[:min(limit, 3)], nil
		},
		GetFunc: func(_ context.Context, id, service string) (domain.StoredEntry, error) {
			if id != entryID {
				return domain.StoredEntry{}, entries.ErrNotFound
			}
			return sample(1, "s:"+service), nil
		},
	}
	h := readerServer(t, reader, &mock.Entries{})
	rec := get(h, "/v1/audit-entries?pageSize=2&service=helpdesk")
	var page struct {
		Items         []map[string]any `json:"items"`
		NextPageToken string           `json:"nextPageToken"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || rec.Code != 200 || len(page.Items) != 2 || page.NextPageToken == "" {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if page.Items[0]["id"] != entryID {
		t.Fatalf("item = %v", page.Items[0])
	}
	// The details keep a number that a float64 cannot hold.
	if !strings.Contains(rec.Body.String(), `"n":12345678901234567890`) {
		t.Fatalf("details changed: %s", rec.Body.String())
	}
	// The token is bound to the filters.
	if rec := get(h, "/v1/audit-entries?pageSize=2&service=crm&pageToken="+page.NextPageToken); rec.Code != 400 || problemCode(t, rec) != "invalid_page_token" {
		t.Fatalf("other filters: %d %s", rec.Code, rec.Body.String())
	}
	if rec := get(h, "/v1/audit-entries?pageSize=2&service=helpdesk&pageToken="+page.NextPageToken); rec.Code != 200 {
		t.Fatalf("same filters: %d %s", rec.Code, rec.Body.String())
	}

	rec = get(h, "/v1/audit-entries/"+strings.ToUpper(entryID)+"?service=crm")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"summary":"s:crm"`) || !strings.Contains(rec.Body.String(), `"id":"`+entryID+`"`) {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
	if rec := get(h, "/v1/audit-entries/0192f4c0-7b1a-7c3e-9a55-000000000000"); rec.Code != 404 || problemCode(t, rec) != "audit_entry_not_found" {
		t.Fatalf("unknown id: %d %s", rec.Code, rec.Body.String())
	}
}

func exportReader(batches ...[]domain.StoredEntry) *mock.Reader {
	n := 0
	for _, b := range batches {
		n += len(b)
	}
	return &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
		return &mock.Session{CountValue: n, Batches: batches}, nil
	}}
}

// U5: the CSV writer, through the handler.
func TestU5_CSVWriter(t *testing.T) {
	cells := []string{"=SUM(A1)", "+1", "-1", "@x", "\tx", "a,b", `say "hi"`, "\nline\nend", ""}
	var batch []domain.StoredEntry
	for i, c := range cells {
		e := sample(int64(100-i), c)
		e.Act, e.ActType = "", ""
		batch = append(batch, e)
	}
	writer := &mock.Entries{}
	rec := get(readerServer(t, exportReader(batch[:5], batch[5:]), writer), "/v1/audit-entries/export?format=csv")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "text/csv" ||
		rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" ||
		rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("status = %d, headers = %v", rec.Code, rec.Header())
	}
	cd := rec.Header().Get("Content-Disposition")
	if !strings.HasPrefix(cd, `attachment; filename="audit-entries-`) || !strings.HasSuffix(cd, `Z.csv"`) || len(cd) != len(`attachment; filename="audit-entries-20261127T101500Z.csv"`) {
		t.Fatalf("Content-Disposition = %q", cd)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, `"id","service","time","receivedAt","action","result","actor","actorType","act","actType",`+
		`"targetType","targetId","summary","requestId","traceId","ipAddress","userAgent","details"`+"\r\n") {
		t.Fatalf("header row: %q", body[:200])
	}
	rows, err := csv.NewReader(strings.NewReader(body)).ReadAll()
	if err != nil || len(rows) != len(cells)+1 {
		t.Fatalf("rows = %d, err = %v", len(rows), err)
	}
	const summaryCol = 12
	want := []string{"'=SUM(A1)", "'+1", "'-1", "'@x", "'\tx", "a,b", `say "hi"`, "'\nline\nend", ""}
	for i, w := range want {
		if rows[i+1][summaryCol] != w {
			t.Fatalf("row %d summary = %q, want %q", i+1, rows[i+1][summaryCol], w)
		}
	}
	if rows[1][0] != entryID || rows[1][2] != "2026-11-27T10:00:00Z" || rows[1][17] != `{"n":12345678901234567890,"a":[1,2]}` || rows[1][8] != "" {
		t.Fatalf("row 1 = %q", rows[1])
	}
	if len(writer.Inserted) != 1 || writer.Inserted[0].Actor != "svc_reader" {
		t.Fatalf("own entry = %+v", writer.Inserted)
	}
}

// U7: the JSON writer with 0, 1, and 3 entries.
func TestU7_JSONWriter(t *testing.T) {
	for _, n := range []int{0, 1, 3} {
		var batch []domain.StoredEntry
		for i := range n {
			batch = append(batch, sample(int64(10-i), "s"))
		}
		var batches [][]domain.StoredEntry
		if n > 0 {
			batches = append(batches, batch[:1], batch[1:])
		}
		rec := get(readerServer(t, exportReader(batches...), &mock.Entries{}), "/v1/audit-entries/export?format=json")
		if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/json" || !strings.HasSuffix(rec.Header().Get("Content-Disposition"), `Z.json"`) {
			t.Fatalf("n = %d: status = %d, headers = %v", n, rec.Code, rec.Header())
		}
		var out []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out) != n {
			t.Fatalf("n = %d: body = %s, err = %v", n, rec.Body.String(), err)
		}
		if n == 0 && rec.Body.String() != "[]" {
			t.Fatalf("body = %q", rec.Body.String())
		}
	}
}

func TestExportProblems(t *testing.T) {
	// 422 with too many entries.
	big := &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
		return &mock.Session{CountValue: 101}, nil
	}}
	writer := &mock.Entries{}
	rec := get(readerServer(t, big, writer), "/v1/audit-entries/export?format=csv")
	if rec.Code != 422 || problemCode(t, rec) != "export_too_large" || rec.Header().Get("Cache-Control") != "no-store" || len(writer.Inserted) != 1 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// 503 when the first batch fails.
	failing := &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) {
		return &mock.Session{CountValue: 1, NextErr: entries.ErrUnavailable}, nil
	}}
	rec = get(readerServer(t, failing, &mock.Entries{}), "/v1/audit-entries/export?format=json")
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("status = %d, headers = %v", rec.Code, rec.Header())
	}
}

// deadlineFail is a response writer whose write deadline cannot change.
type deadlineFail struct{ *httptest.ResponseRecorder }

func (deadlineFail) SetWriteDeadline(time.Time) error { return io.ErrClosedPipe }

// R10: an error of SetWriteDeadline stops the export before the first byte,
// frees the slot, and gives 500.
func TestExportWriteDeadlineError(t *testing.T) {
	session := &mock.Session{CountValue: 1, Batches: [][]domain.StoredEntry{{sample(1, "s")}}}
	reader := &mock.Reader{BeginExportFunc: func(context.Context, domain.Filters) (entries.ExportSession, error) { return session, nil }}
	writer := &mock.Entries{}
	h := readerServer(t, reader, writer)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/audit-entries/export?format=csv", nil)
	req.Header.Set("Authorization", "Bearer reader")
	rec := deadlineFail{httptest.NewRecorder()}
	h.ServeHTTP(rec, req)
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "svc_reader") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if session.Closed() != 1 {
		t.Fatalf("closed = %d", session.Closed())
	}
	// The own entry of the start and the failure entry.
	if len(writer.Inserted) != 2 || writer.Inserted[1].Result != "failure" {
		t.Fatalf("entries = %+v", writer.Inserted)
	}
}
