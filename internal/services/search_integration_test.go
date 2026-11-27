//go:build integration

package services_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/techorionai/constellation-platform-go/audit"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/techorionai/constellation-audit/internal/config"
	"github.com/techorionai/constellation-audit/internal/handlers/http/gen"
	"github.com/techorionai/constellation-audit/resources/test/testserver"
)

const (
	listPath   = "/v1/audit-entries"
	exportPath = "/v1/audit-entries/export"
	exportRows = `SELECT count(*) FROM audit_entries WHERE action = 'audit:log:export'`
)

// row is an audit entry that a test writes into the table with SQL. An empty
// optional value is NULL.
type row struct {
	Service, EntryID, Action, Result, Actor, ActorType string
	Act, ActType, TargetType, TargetID, Summary        string
	Details, RequestID, TraceID, IP, UserAgent         string
	Time                                               time.Time
}

func nul(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// insertRows writes rows into the table. The default service is helpdesk.
func insertRows(t *testing.T, p *pgxpool.Pool, rows ...row) {
	t.Helper()
	for _, r := range rows {
		svc := or(r.Service, "helpdesk")
		id := or(r.EntryID, uuid.NewString())
		tm := r.Time
		if tm.IsZero() {
			tm = time.Now().UTC()
		}
		_, err := p.Exec(context.Background(), `
			INSERT INTO audit_entries (entry_id, service, time, action, result, actor, actor_type, act, act_type,
				target_type, target_id, summary, details, request_id, trace_id, ip_address, user_agent)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15, $16::text::inet, $17)`,
			id, svc, tm, or(r.Action, svc+":ticket:write"), or(r.Result, "success"), or(r.Actor, "usr_1"), or(r.ActorType, "user"),
			nul(r.Act), nul(r.ActType), nul(r.TargetType), nul(r.TargetID), nul(r.Summary), nul(r.Details),
			nul(r.RequestID), nul(r.TraceID), nul(r.IP), nul(r.UserAgent))
		if err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}
}

// seedBulk writes n entries of a service. The time of entry g is
// newest - (g/2) seconds, so that 2 entries share each time.
func seedBulk(t *testing.T, p *pgxpool.Pool, service string, n int, newest time.Time, summaryLen, detailsLen int) {
	t.Helper()
	_, err := p.Exec(context.Background(), `
		INSERT INTO audit_entries (entry_id, service, time, action, result, actor, actor_type, summary, details)
		SELECT gen_random_uuid(), $1, $2::timestamptz - (g / 2) * interval '1 second', $3, 'success', 'usr_1', 'user',
			repeat('s', $4::int), CASE WHEN $5::int = 0 THEN NULL ELSE jsonb_build_object('t', repeat('d', $5::int)) END
		FROM generate_series(1, $6::int) g`,
		service, newest, service+":ticket:write", summaryLen, detailsLen, n)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

type entryPage struct {
	Items []gen.AuditEntry `json:"items"`
	Next  *string          `json:"nextPageToken"`
}

func readToken(t *testing.T) string {
	t.Helper()
	return env.Token(t, "reader-svc", "audit", readScope)
}

func page(t *testing.T, srv *testserver.Server, token, query string) entryPage {
	t.Helper()
	resp := srv.Do(t, http.MethodGet, listPath+query, token, nil)
	if resp.Status != http.StatusOK {
		t.Fatalf("GET %s: status = %d, body = %s", query, resp.Status, resp.Body)
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Fatalf("GET %s: headers = %v", query, resp.Header)
	}
	var p entryPage
	resp.JSON(t, &p)
	return p
}

func summaries(items []gen.AuditEntry) []string {
	var out []string
	for _, e := range items {
		if e.Summary != nil && *e.Summary != "Export of audit entries" {
			out = append(out, *e.Summary)
		}
	}
	return out
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	m := map[string]int{}
	for _, s := range got {
		m[s]++
	}
	for _, s := range want {
		m[s]--
	}
	for _, n := range m {
		if n != 0 {
			return false
		}
	}
	return true
}

// I1 of issue 2 (test SI1): 120 entries of helpdesk and 30 of crm, pageSize 50, filter
// service=helpdesk. 3 pages, newest first, only helpdesk, each entry one time.
func TestSearchI1_Pagination(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	base := time.Now().UTC().Add(-time.Hour)
	seedBulk(t, p, "helpdesk", 120, base, 10, 0)
	seedBulk(t, p, "crm", 30, base, 10, 0)
	token := readToken(t)

	var all []gen.AuditEntry
	query := "?pageSize=50&service=helpdesk"
	pages := 0
	for {
		pg := page(t, srv, token, query)
		pages++
		all = append(all, pg.Items...)
		if pg.Next == nil {
			break
		}
		query = "?pageSize=50&service=helpdesk&pageToken=" + url.QueryEscape(*pg.Next)
		if pages > 5 {
			t.Fatal("too many pages")
		}
	}
	if pages != 3 || len(all) != 120 {
		t.Fatalf("pages = %d, items = %d", pages, len(all))
	}
	seen := map[uuid.UUID]bool{}
	for i, e := range all {
		if e.Service != "helpdesk" || seen[e.Id] || (i > 0 && e.Time.After(all[i-1].Time)) {
			t.Fatalf("item %d is not valid: %+v", i, e)
		}
		seen[e.Id] = true
	}
	// A token for other filters, and a bad token, give 400.
	pg := page(t, srv, token, "?pageSize=50&service=helpdesk")
	for _, q := range []string{
		"?pageSize=50&service=crm&pageToken=" + url.QueryEscape(*pg.Next),
		"?pageToken=" + strings.Repeat("a", 1025),
		"?pageToken=%21%21",
	} {
		if resp := srv.Do(t, http.MethodGet, listPath+q, token, nil); resp.Status != http.StatusBadRequest || resp.Code(t) != "invalid_page_token" {
			t.Fatalf("%.60s: status = %d, body = %s", q, resp.Status, resp.Body)
		}
	}
}

// I2 of issue 2 (test SI2): each filter alone. From is inclusive and to is exclusive at an exact
// entry time. A bad text value gives 400 invalid_parameter.
func TestSearchI2_Filters(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	tr1, tr2 := strings.Repeat("a", 32), strings.Repeat("b", 32)
	insertRows(t, p,
		row{Summary: "r1", Time: t0, Action: "helpdesk:ticket:write", Result: "success", Actor: "usr_1", ActorType: "user", Act: "agent_1", ActType: "agent",
			TargetType: "ticket", TargetID: "t1", RequestID: "req-1", TraceID: tr1},
		row{Summary: "r2", Time: t0.Add(time.Second), Service: "crm", Action: "crm:contact:read", Result: "denied", Actor: "svc_1", ActorType: "service",
			TargetType: "contact", TargetID: "c1", RequestID: "req-2", TraceID: tr2},
		row{Summary: "r3", Time: t0.Add(2 * time.Second), Action: "helpdesk:ticket:read", Result: "failure", Actor: "usr_2", ActorType: "user",
			TargetType: "ticket", TargetID: "t2"},
		row{Summary: "r4", Time: t0.Add(3 * time.Second), Action: "helpdesk:ticket:write", Result: "invalid", Actor: "agent_9", ActorType: "agent", Act: "usr_1", ActType: "user"},
	)
	token := readToken(t)
	at := func(d time.Duration) string { return url.QueryEscape(t0.Add(d).Format(time.RFC3339Nano)) }
	cases := []struct {
		query string
		want  []string
	}{
		{"", []string{"r1", "r2", "r3", "r4"}},
		{"from=" + at(time.Second), []string{"r2", "r3", "r4"}},
		{"to=" + at(time.Second), []string{"r1"}},
		{"from=" + at(time.Second) + "&to=" + at(3*time.Second), []string{"r2", "r3"}},
		{"service=crm", []string{"r2"}},
		{"action=helpdesk:ticket:write", []string{"r1", "r4"}},
		{"result=denied", []string{"r2"}},
		{"actor=usr_2", []string{"r3"}},
		{"actorType=agent", []string{"r4"}},
		{"act=agent_1", []string{"r1"}},
		{"targetType=ticket", []string{"r1", "r3"}},
		{"targetType=ticket&targetId=t2", []string{"r3"}},
		{"requestId=req-2", []string{"r2"}},
		{"traceId=" + tr1, []string{"r1"}},
		{"service=helpdesk&result=success&actorType=user", []string{"r1"}},
		{"service=billing", nil},
	}
	for _, tc := range cases {
		pg := page(t, srv, token, "?"+tc.query)
		if !sameSet(summaries(pg.Items), tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.query, summaries(pg.Items), tc.want)
		}
		// The export has the same filters.
		resp := srv.Do(t, http.MethodGet, exportPath+"?format=json&"+tc.query, token, nil)
		var out []gen.AuditEntry
		resp.JSON(t, &out)
		if resp.Status != http.StatusOK || !sameSet(summaries(out), tc.want) {
			t.Fatalf("export %s: status = %d, got %v, want %v", tc.query, resp.Status, summaries(out), tc.want)
		}
	}
	for _, q := range []string{
		"actor=%00", "targetType=ticket&targetId=%FF", "targetId=t1", "from=" + at(time.Second) + "&to=" + at(time.Second),
		"result=ok", "traceId=ABC", "service=Bad-App", "actorType=robot", "from=yesterday",
	} {
		for _, path := range []string{listPath + "?" + q, exportPath + "?format=csv&" + q} {
			if resp := srv.Do(t, http.MethodGet, path, token, nil); resp.Status != http.StatusBadRequest || resp.Code(t) != "invalid_parameter" {
				t.Fatalf("%s: status = %d, body = %s", path, resp.Status, resp.Body)
			}
		}
	}
}

// I3 of issue 2 (test SI3): new entries arrive between page 1 and page 2. Page 2 has no entry of
// page 1 and no new entry.
func TestSearchI3_StablePages(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	seedBulk(t, p, "helpdesk", 30, time.Now().UTC().Add(-time.Hour), 5, 0)
	token := readToken(t)
	first := page(t, srv, token, "?pageSize=10")
	if first.Next == nil || len(first.Items) != 10 {
		t.Fatalf("page 1 = %d items, next = %v", len(first.Items), first.Next)
	}
	for range 5 {
		insertRows(t, p, row{Summary: "new", Time: time.Now().UTC()})
	}
	second := page(t, srv, token, "?pageSize=10&pageToken="+url.QueryEscape(*first.Next))
	if len(second.Items) != 10 {
		t.Fatalf("page 2 = %d items", len(second.Items))
	}
	ids := map[uuid.UUID]bool{}
	for _, e := range first.Items {
		ids[e.Id] = true
	}
	for _, e := range second.Items {
		if ids[e.Id] || (e.Summary != nil && *e.Summary == "new") {
			t.Fatalf("page 2 has an entry of page 1 or a new entry: %+v", e)
		}
	}
}

// I4 of issue 2 (test SI4): the get of a known ID, of the same ID in upper case, of an unknown
// UUID, and of a value that is not a UUID.
func TestSearchI4_Get(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	id := uuid.NewString()
	insertRows(t, p, row{EntryID: id, Summary: "known", Act: "agent_1", ActType: "agent", TargetType: "ticket", TargetID: "t1",
		Details: `{"k":"v","n":1}`, RequestID: "req-1", TraceID: strings.Repeat("c", 32), IP: "203.0.113.5", UserAgent: "ua-1"})
	token := readToken(t)
	for _, path := range []string{listPath + "/" + id, listPath + "/" + strings.ToUpper(id)} {
		resp := srv.Do(t, http.MethodGet, path, token, nil)
		if resp.Status != http.StatusOK || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
			t.Fatalf("%s: status = %d, headers = %v, body = %s", path, resp.Status, resp.Header, resp.Body)
		}
		var e gen.AuditEntry
		resp.JSON(t, &e)
		if e.Id.String() != id || e.Service != "helpdesk" || *e.Summary != "known" || *e.Act != "agent_1" || *e.ActType != "agent" ||
			e.Target == nil || e.Target.Id != "t1" || *e.IpAddress != "203.0.113.5" || *e.UserAgent != "ua-1" ||
			*e.RequestId != "req-1" || e.Details == nil || (*e.Details)["k"] != "v" || e.ReceivedAt.IsZero() {
			t.Fatalf("%s: entry = %+v", path, e)
		}
		if !strings.Contains(string(resp.Body), `"id":"`+id+`"`) {
			t.Fatalf("the id is not in lowercase: %s", resp.Body)
		}
	}
	for _, path := range []string{listPath + "/" + uuid.NewString(), listPath + "/abc"} {
		if resp := srv.Do(t, http.MethodGet, path, token, nil); resp.Status != http.StatusNotFound || resp.Code(t) != "audit_entry_not_found" {
			t.Fatalf("%s: status = %d, body = %s", path, resp.Status, resp.Body)
		}
	}
}

// producerEntries writes 3 entries with the audit writer and waits until the
// intake stores them.
func producerEntries(t *testing.T, srv *testserver.Server) {
	t.Helper()
	pr := env.NewProducer(t, "helpdesk")
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	pr.Write(t,
		audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success, Summary: "=cmd", Time: base.Add(2 * time.Second), Target: &audit.Target{Type: "ticket", ID: "t_1"}, Details: map[string]any{"k": "v"}},
		audit.Entry{Action: "helpdesk:ticket:read", Result: audit.Denied, Summary: "second", Time: base.Add(time.Second)},
		audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Failure, Summary: "third", Time: base},
	)
	waitCount(t, pool(t, srv.DB), 3, selectRows)
}

// I5 of issue 2 (test SI5): the CSV export of 3 entries, with a summary =cmd and 3 different times.
func TestSearchI5_ExportCSV(t *testing.T) {
	srv := started(t)
	producerEntries(t, srv)
	token := readToken(t)
	resp := srv.Do(t, http.MethodGet, exportPath+"?format=csv", token, nil)
	if resp.Status != http.StatusOK || resp.Header.Get("Content-Type") != "text/csv" || resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
		resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Fatalf("status = %d, headers = %v, body = %s", resp.Status, resp.Header, resp.Body)
	}
	cd := resp.Header.Get("Content-Disposition")
	if !strings.HasPrefix(cd, `attachment; filename="audit-entries-`) || !strings.HasSuffix(cd, `Z.csv"`) {
		t.Fatalf("Content-Disposition = %q", cd)
	}
	rows, err := csv.NewReader(bytes.NewReader(resp.Body)).ReadAll()
	if err != nil || len(rows) != 4 {
		t.Fatalf("rows = %d, err = %v, body = %s", len(rows), err, resp.Body)
	}
	if strings.Join(rows[0], ",") != "id,service,time,receivedAt,action,result,actor,actorType,act,actType,targetType,targetId,summary,requestId,traceId,ipAddress,userAgent,details" {
		t.Fatalf("header = %v", rows[0])
	}
	if rows[1][12] != "'=cmd" || rows[2][12] != "second" || rows[3][12] != "third" || rows[1][1] != "helpdesk" || rows[1][10] != "ticket" || rows[1][17] != `{"k": "v"}` {
		t.Fatalf("rows = %q", rows)
	}
	var count int
	var details string
	if err := pool(t, srv.DB).QueryRow(context.Background(),
		`SELECT count(*), max(details::text) FROM audit_entries WHERE action = 'audit:log:export' AND result = 'success'`).Scan(&count, &details); err != nil {
		t.Fatal(err)
	}
	if count != 1 || !strings.Contains(details, `"count": 3`) || !strings.Contains(details, `"format": "csv"`) {
		t.Fatalf("own entries = %d, details = %s", count, details)
	}
}

// I6 of issue 2 (test SI6): the JSON export of the same entries.
func TestSearchI6_ExportJSON(t *testing.T) {
	srv := started(t)
	producerEntries(t, srv)
	resp := srv.Do(t, http.MethodGet, exportPath+"?format=json", readToken(t), nil)
	if resp.Status != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" || !strings.HasSuffix(resp.Header.Get("Content-Disposition"), `Z.json"`) {
		t.Fatalf("status = %d, headers = %v", resp.Status, resp.Header)
	}
	dec := json.NewDecoder(bytes.NewReader(resp.Body))
	dec.DisallowUnknownFields()
	var out []gen.AuditEntry
	if err := dec.Decode(&out); err != nil || len(out) != 3 {
		t.Fatalf("out = %d, err = %v, body = %s", len(out), err, resp.Body)
	}
	if got := summaries(out); got[0] != "=cmd" || got[1] != "second" || got[2] != "third" {
		t.Fatalf("order = %v", got)
	}
	if out[0].Target == nil || out[0].Target.Type != "ticket" || out[0].Details == nil || (*out[0].Details)["k"] != "v" || out[0].Result != gen.AuditResult("success") {
		t.Fatalf("entry = %+v", out[0])
	}
}

// I7 of issue 2 (test SI7): an export with AUDIT_EXPORT__MAX_ENTRIES=2 and 3 entries.
func TestSearchI7_ExportTooLarge(t *testing.T) {
	srv := started(t, testserver.WithConfigure(func(c *config.Config) { c.Export.MaxEntries = 2 }))
	p := pool(t, srv.DB)
	newest := time.Now().UTC().Add(-time.Hour)
	seedBulk(t, p, "helpdesk", 3, newest, 5, 0)
	token := readToken(t)
	resp := srv.Do(t, http.MethodGet, exportPath+"?format=csv&service=helpdesk", token, nil)
	if resp.Status != http.StatusUnprocessableEntity || resp.Code(t) != "export_too_large" || resp.Header.Get("Content-Type") != "application/problem+json" ||
		resp.Header.Get("Cache-Control") != "no-store" || strings.Contains(string(resp.Body), "usr_1") {
		t.Fatalf("status = %d, headers = %v, body = %s", resp.Status, resp.Header, resp.Body)
	}
	var details string
	if err := p.QueryRow(context.Background(), `SELECT details::text FROM audit_entries WHERE action = 'audit:log:export' AND result = 'invalid'`).Scan(&details); err != nil {
		t.Fatalf("no invalid entry: %v", err)
	}
	if !strings.Contains(details, `"maxEntries": 2`) {
		t.Fatalf("details = %s", details)
	}
	// 2 entries are in the limit.
	if resp := srv.Do(t, http.MethodGet, exportPath+"?format=csv&service=helpdesk&to="+url.QueryEscape(newest.Format(time.RFC3339Nano)), token, nil); resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
}

// countJSON counts the elements of a JSON array in a stream.
func countJSON(r io.Reader) (int, error) {
	dec := json.NewDecoder(r)
	if _, err := dec.Token(); err != nil {
		return 0, err
	}
	n := 0
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return n, err
		}
		n++
	}
	_, err := dec.Token()
	return n, err
}

// I8 of issue 2 (test SI8): concurrency 2. 2 exports of 5000 large entries run, and the clients do
// not read the body, so the server blocks on the write. A third export gives
// 429. Then the clients read the 2 bodies.
func TestSearchI8_ExportConcurrency(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	seedBulk(t, p, "helpdesk", 5000, time.Now().UTC().Add(-time.Hour), 1000, 15000)
	token := readToken(t)
	a := srv.Open(t, exportPath+"?format=json&service=helpdesk", token, nil)
	defer func() { _ = a.Body.Close() }()
	b := srv.Open(t, exportPath+"?format=json&service=helpdesk", token, nil)
	defer func() { _ = b.Body.Close() }()
	if a.StatusCode != http.StatusOK || b.StatusCode != http.StatusOK {
		t.Fatalf("status = %d and %d", a.StatusCode, b.StatusCode)
	}
	third := srv.Do(t, http.MethodGet, exportPath+"?format=json&service=helpdesk", token, nil)
	if third.Status != http.StatusTooManyRequests || third.Code(t) != "too_many_exports" || third.Header.Get("Retry-After") != "10" {
		t.Fatalf("third: status = %d, headers = %v, body = %s", third.Status, third.Header, third.Body)
	}
	if got := count(t, p, `SELECT count(*) FROM audit_entries WHERE action = 'audit:log:export' AND result = 'denied'`); got != 1 {
		t.Fatalf("denied entries = %d, want 1", got)
	}
	var wg sync.WaitGroup
	counts := make([]int, 2)
	errs := make([]error, 2)
	for i, r := range []*http.Response{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counts[i], errs[i] = countJSON(r.Body)
		}()
	}
	wg.Wait()
	for i := range counts {
		if errs[i] != nil || counts[i] != 5000 {
			t.Fatalf("export %d: count = %d, err = %v", i, counts[i], errs[i])
		}
	}
	// The slots are free again.
	if resp := srv.Do(t, http.MethodGet, exportPath+"?format=csv&service=none", token, nil); resp.Status != http.StatusOK {
		t.Fatalf("after the exports: status = %d, body = %s", resp.Status, resp.Body)
	}
}

// I9 of issue 2 (test SI9): PostgreSQL stops during a list and during the body of an export of 50000
// entries.
func TestSearchI9_StoreStops(t *testing.T) {
	db, proxy := env.NewProxiedDB(t)
	srv := started(t, testserver.WithDB(db))
	p := pool(t, srv.DB)
	seedBulk(t, p, "helpdesk", 50000, time.Now().UTC().Add(-time.Hour), 1000, 0)
	token := readToken(t)
	if pg := page(t, srv, token, "?pageSize=5"); len(pg.Items) != 5 {
		t.Fatalf("items = %d", len(pg.Items))
	}

	resp := srv.Open(t, exportPath+"?format=csv", token, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("export status = %d", resp.StatusCode)
	}
	head := make([]byte, 1<<20)
	if _, err := io.ReadFull(resp.Body, head); err != nil {
		t.Fatal(err)
	}
	proxy.Stop()
	rest, err := io.Copy(io.Discard, resp.Body)
	if err == nil {
		t.Fatalf("the body ended with no error after %d more bytes, want an incomplete body", rest)
	}

	list := srv.Do(t, http.MethodGet, listPath, token, nil)
	if list.Status != http.StatusServiceUnavailable || list.Code(t) != "unavailable" || list.Header.Get("Retry-After") != "5" {
		t.Fatalf("list: status = %d, headers = %v, body = %s", list.Status, list.Header, list.Body)
	}
	proxy.Start()
}

// I10 of issue 2 is TestI10_Authorization of the base issue. It now checks the
// 3 read routes: 200 or 404 for a token with the scope, 401 for no token and for
// another audience. The 403 case with missingScopes is in the unit test U5.

// markers are values that must not be in a log record.
var markers = []string{"zz-actor-marker", "zz-summary-marker", "zz-details-marker", "zz-agent-marker", "203.0.113.77", "zz-request-marker"}

// I11 of issue 2 (test SI11): no log record has a filter value, a summary, details, an IP address,
// or a user agent.
func TestSearchI11_NoDataInLogs(t *testing.T) {
	logger, capture := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger), testserver.WithConfigure(func(c *config.Config) {
		c.HTTP.TrustedProxies = "127.0.0.0/8"
		c.Export.MaxEntries = 3
	}))
	p := pool(t, srv.DB)
	for range 4 {
		insertRows(t, p, row{Actor: "zz-actor-marker", Summary: "zz-summary-marker", Details: `{"k":"zz-details-marker"}`,
			RequestID: "zz-request-marker", IP: "203.0.113.77", UserAgent: "zz-agent-marker", Act: "zz-actor-marker", ActType: "user"})
	}
	token := readToken(t)
	hdr := map[string]string{"User-Agent": "zz-agent-marker", "X-Forwarded-For": "203.0.113.77", "X-Request-ID": "zz-request-marker"}
	paths := []string{
		listPath + "?actor=zz-actor-marker&requestId=zz-request-marker",
		listPath + "?actor=zz-actor-marker&pageSize=2",
		listPath + "?actor=%00zz-actor-marker",
		listPath + "?targetType=ticket&targetId=%FFzz-actor-marker",
		exportPath + "?format=json&actor=zz-actor-marker",
		exportPath + "?format=csv&actor=zz-actor-marker&limit=1",
		exportPath + "?format=csv&act=zz-actor-marker&service=helpdesk&to=2000-01-01T00:00:00Z",
		listPath + "/" + uuid.NewString(),
	}
	for _, path := range paths {
		srv.DoHeader(t, path, token, hdr)
	}
	// The 3 entries of the limit: only the export of 3 or fewer entries works.
	srv.DoHeader(t, exportPath+"?format=csv&act=zz-actor-marker&service=helpdesk&pageSize=1&to=2000-01-01T00:00:00Z", token, hdr)
	text := capture.Text()
	if !strings.Contains(text, "audit export") {
		t.Fatalf("the log has no record of an export, so the test proves nothing:\n%s", text)
	}
	for _, m := range markers {
		if strings.Contains(text, m) {
			t.Fatalf("the log has %q:\n%s", m, text)
		}
	}
}

// I12 of issue 2 (test SI12): the own entry of an export has the request data. Without a trusted
// proxy, ip_address is the address of the client. With the trusted proxy, it is
// the address in X-Forwarded-For.
func TestSearchI12_OwnEntry(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	hdr := map[string]string{
		"X-Request-ID": "req-1", "User-Agent": "test-agent", "X-Forwarded-For": "203.0.113.9",
		"traceparent": "00-" + traceID + "-00f067aa0ba902b7-01",
	}
	for _, trusted := range []bool{false, true} {
		srv := started(t, testserver.WithConfigure(func(c *config.Config) {
			if trusted {
				c.HTTP.TrustedProxies = "127.0.0.1/32,::1/128"
			}
		}))
		p := pool(t, srv.DB)
		insertRows(t, p, row{Summary: "x"})
		resp := srv.DoHeader(t, exportPath+"?format=csv&service=helpdesk&actor=usr_1", readToken(t), hdr)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
		}
		var (
			id, svc, action, result, actor, actorType, req, trace, ip, ua, details string
			act                                                                    *string
		)
		err := p.QueryRow(context.Background(), `
			SELECT entry_id::text, service, action, result, actor, actor_type, act, request_id, trace_id, host(ip_address), user_agent, details::text
			FROM audit_entries WHERE action = 'audit:log:export'`).Scan(&id, &svc, &action, &result, &actor, &actorType, &act, &req, &trace, &ip, &ua, &details)
		if err != nil {
			t.Fatalf("trusted = %v: %v", trusted, err)
		}
		parsed, perr := uuid.Parse(id)
		if perr != nil || parsed.Version() != 7 || id != strings.ToLower(id) {
			t.Fatalf("entry_id = %q", id)
		}
		wantIP := "127.0.0.1"
		if trusted {
			wantIP = "203.0.113.9"
		}
		if svc != "audit" || result != "success" || actor != "reader-svc" || actorType != "service" || act != nil ||
			req != "req-1" || trace != traceID || ua != "test-agent" || ip != wantIP ||
			!strings.Contains(details, `"count": 1`) || !strings.Contains(details, `"format": "csv"`) || !strings.Contains(details, `"actor": "usr_1"`) {
			t.Fatalf("trusted = %v: entry = %s %s %s %s %v req=%s trace=%s ip=%s ua=%s details=%s", trusted, svc, result, actor, actorType, act, req, trace, ip, ua, details)
		}
		srv.Stop()
	}
}

// I13 of issue 2 (test SI13): the same entry_id from helpdesk and crm. The get with service selects
// one. Without service, the get returns the entry with the oldest receipt time.
func TestSearchI13_GetWithService(t *testing.T) {
	srv := started(t)
	p := pool(t, srv.DB)
	id := uuid.NewString()
	insertRows(t, p, row{EntryID: id, Service: "helpdesk", Summary: "from helpdesk"})
	time.Sleep(20 * time.Millisecond)
	insertRows(t, p, row{EntryID: id, Service: "crm", Summary: "from crm"})
	token := readToken(t)
	cases := []struct {
		query  string
		status int
		want   string
		code   string
	}{
		{"?service=helpdesk", 200, "from helpdesk", ""},
		{"?service=crm", 200, "from crm", ""},
		{"", 200, "from helpdesk", ""},
		{"?service=billing", 404, "", "audit_entry_not_found"},
		{"?service=Bad-App", 400, "", "invalid_parameter"},
	}
	for _, tc := range cases {
		resp := srv.Do(t, http.MethodGet, listPath+"/"+id+tc.query, token, nil)
		if resp.Status != tc.status {
			t.Fatalf("%q: status = %d, body = %s", tc.query, resp.Status, resp.Body)
		}
		if tc.code != "" && resp.Code(t) != tc.code {
			t.Fatalf("%q: code = %s", tc.query, resp.Code(t))
		}
		if tc.want != "" {
			var e gen.AuditEntry
			resp.JSON(t, &e)
			if *e.Summary != tc.want {
				t.Fatalf("%q: summary = %q, want %q", tc.query, *e.Summary, tc.want)
			}
		}
	}
}
