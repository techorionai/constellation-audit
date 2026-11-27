//go:build integration

package services_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/audit"
	"github.com/techorionai/constellation-platform-go/resources/test/containers"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/techorionai/constellation-audit/internal/config"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/resources/test/testserver"
)

const (
	entriesCounter = "constellation.audit.intake.entries"
	selectRows     = `SELECT count(*) FROM audit_entries`
	selectRejected = `SELECT count(*) FROM rejected_entries`
)

// I1: at start the audit service publishes its manifest to identity. /readyz
// is 503 until the publish succeeds.
func TestI1_StartupPublishesManifest(t *testing.T) {
	identityURL, err := url.Parse(env.IssuerURL)
	if err != nil {
		t.Fatal(err)
	}
	open := make(chan struct{})
	proxy := httputil.NewSingleHostReverseProxy(identityURL)
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-open:
			proxy.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	defer gate.Close()

	env.ResetAuditStreams(t)
	srv := env.Start(t, testserver.WithConfigure(func(cfg *config.Config) { cfg.Client.TokenURL = gate.URL + "/oauth/token" }))
	for range 5 {
		if got := srv.Ready(); got != http.StatusServiceUnavailable {
			t.Fatalf("/readyz before the publish = %d, want 503", got)
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(open)
	srv.WaitReady(t)

	// Identity gives a token for the audience audit only when it has the manifest.
	token := env.Token(t, "reader-svc", "audit", readScope)
	resp := srv.Do(t, http.MethodGet, "/v1/audit-entries", token, nil)
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
}

// I2: the producer helpdesk writes 3 entries with the audit writer. The intake
// stores them with the fields of the entries.
func TestI2_ProducerEntriesStored(t *testing.T) {
	srv := started(t)
	p := env.NewProducer(t, "helpdesk")
	ids := p.Write(t,
		audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success, Target: &audit.Target{Type: "ticket", ID: "t_1"}, Summary: "Ticket 1", Details: map[string]any{"k": "v"}},
		audit.Entry{Action: "helpdesk:ticket:read", Result: audit.Denied},
		audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Failure, Summary: "Ticket 3"},
	)
	db := pool(t, srv.DB)
	waitCount(t, db, 3, selectRows)

	var (
		service, action, result, actor, actorType, summary, targetType, targetID string
		details                                                                  string
	)
	err := db.QueryRow(context.Background(), `
		SELECT service, action, result, actor, actor_type, coalesce(summary, ''), coalesce(target_type, ''), coalesce(target_id, ''), coalesce(details::text, '')
		FROM audit_entries WHERE entry_id = $1`, ids[0]).
		Scan(&service, &action, &result, &actor, &actorType, &summary, &targetType, &targetID, &details)
	if err != nil {
		t.Fatal(err)
	}
	if service != "helpdesk" || action != "helpdesk:ticket:write" || result != "success" || actor != "helpdesk-svc" ||
		actorType != "service" || summary != "Ticket 1" || targetType != "ticket" || targetID != "t_1" || details != `{"k": "v"}` {
		t.Fatalf("row = %s %s %s %s %s %q %s %s %s", service, action, result, actor, actorType, summary, targetType, targetID, details)
	}
	if n := count(t, db, `SELECT count(*) FROM audit_entries WHERE service = 'helpdesk' AND received_at IS NOT NULL AND time <= received_at`); n != 3 {
		t.Fatalf("rows with a receive time = %d, want 3", n)
	}
	waitNoPending(t, p)
}

// I3: the same CloudEvent two times on the stream gives one row. The counter has
// one stored and one duplicate.
func TestI3_Duplicate(t *testing.T) {
	mp, reader := meterOf(t)
	srv := started(t, testserver.WithMeterProvider(mp))
	p := env.NewProducer(t, "helpdesk")
	payload := rawEvent(t, "helpdesk", nil)
	p.PublishRaw(t, payload, payload)
	db := pool(t, srv.DB)
	waitCount(t, db, 1, selectRows)
	waitNoPending(t, p)
	if got := counter(t, reader, entriesCounter, "outcome", "stored"); got != 1 {
		t.Fatalf("stored = %d, want 1", got)
	}
	if got := counter(t, reader, entriesCounter, "outcome", "duplicate"); got != 1 {
		t.Fatalf("duplicate = %d, want 1", got)
	}
}

// I4: an entry with an action of another app and an entry of 64 KiB plus 1 byte
// go to rejected_entries. The messages are acknowledged.
func TestI4_RejectedEntries(t *testing.T) {
	srv := started(t)
	p := env.NewProducer(t, "helpdesk")
	badAction := rawEvent(t, "helpdesk", func(_, data map[string]any) { data["action"] = "crm:ticket:write" })
	tooLarge := rawEvent(t, "helpdesk", func(envelope, _ map[string]any) { envelope["pad"] = strings.Repeat("a", 64<<10) })
	if len(tooLarge) <= domain.MaxEntryBytes {
		t.Fatalf("the large entry has %d bytes", len(tooLarge))
	}
	p.PublishRaw(t, badAction, tooLarge)
	db := pool(t, srv.DB)
	waitCount(t, db, 2, selectRejected)
	if n := count(t, db, selectRows); n != 0 {
		t.Fatalf("audit_entries has %d rows, want 0", n)
	}
	for _, reason := range []string{"invalid_action", "too_large"} {
		if n := count(t, db, `SELECT count(*) FROM rejected_entries WHERE reason = $1 AND stream = 'helpdesk'`, reason); n != 1 {
			t.Fatalf("rejected_entries with the reason %s = %d, want 1", reason, n)
		}
	}
	waitNoPending(t, p)
}

// I5: PostgreSQL stops while the producer writes 2 entries. No entry is lost.
// After the return of PostgreSQL, there are 2 rows.
func TestI5_DatabaseStopped(t *testing.T) {
	db, proxy := env.NewProxiedDB(t)
	logger, logs := testserver.NewLogCapture()
	srv := started(t, testserver.WithDB(db), testserver.WithLogger(logger))
	p := env.NewProducer(t, "helpdesk")
	first := p.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success, Summary: "SECRET-SUMMARY-5"})
	direct := pool(t, srv.DB)
	waitCount(t, direct, 1, selectRows)

	proxy.Stop()
	p.Write(t,
		audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success, Summary: "SECRET-SUMMARY-5"},
		audit.Entry{Action: "helpdesk:ticket:read", Result: audit.Success},
	)
	time.Sleep(2 * time.Second)
	proxy.Start()

	waitCount(t, direct, 3, selectRows)
	waitNoPending(t, p)
	if n := count(t, direct, selectRejected); n != 0 {
		t.Fatalf("rejected_entries has %d rows, want 0", n)
	}
	if n := count(t, direct, `SELECT count(*) FROM audit_entries WHERE entry_id = $1`, first[0]); n != 1 {
		t.Fatalf("the first entry has %d rows", n)
	}
	if strings.Contains(logs.Text(), "SECRET-SUMMARY-5") {
		t.Fatal("a log record has the summary of an entry")
	}
}

// I6: a producer that writes its first entry after the start of the service is
// read within 2 refresh intervals.
func TestI6_NewProducer(t *testing.T) {
	srv := started(t)
	db := pool(t, srv.DB)
	p := env.NewProducer(t, "crm")
	start := time.Now()
	p.Write(t, audit.Entry{Action: "crm:contact:write", Result: audit.Success})
	waitCount(t, db, 1, `SELECT count(*) FROM audit_entries WHERE service = 'crm'`)
	// The refresh interval is 500 ms. The read of the stream blocks for up to
	// 5 seconds, but a new consumer starts at once.
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the entry came after %s", d)
	}
}

// I7: a producer writes 5 entries before the service starts. The service reads
// them from the start of the stream.
func TestI7_EntriesBeforeStart(t *testing.T) {
	env.ResetAuditStreams(t)
	p := env.NewProducer(t, "helpdesk")
	for range 5 {
		p.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	}
	srv := env.Start(t)
	srv.WaitReady(t)
	waitCount(t, pool(t, srv.DB), 5, selectRows)
}

// I10: a token of reader-svc gives 200 on the list and the export, and 404 on
// the get of an unknown entry. A token without the scope
// gives 403 with missingScopes. A token for another audience gives 401.
func TestI10_Authorization(t *testing.T) {
	srv := started(t)
	routes := map[string]int{
		"/v1/audit-entries":                                      http.StatusOK,
		"/v1/audit-entries/export?format=csv":                    http.StatusOK,
		"/v1/audit-entries/0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88": http.StatusNotFound,
	}
	reader := env.Token(t, "reader-svc", "audit", readScope)
	for route, want := range routes {
		resp := srv.Do(t, http.MethodGet, route, reader, nil)
		if resp.Status != want {
			t.Fatalf("%s: status = %d, body = %s", route, resp.Status, resp.Body)
		}
		if resp := srv.Do(t, http.MethodGet, route, "", nil); resp.Status != http.StatusUnauthorized {
			t.Fatalf("%s without a token: status = %d", route, resp.Status)
		}
	}

	// Identity gives a token only with the scopes of the roles of the client.
	// A client with no role cannot get a token with the read scope, so the
	// 403 case with missingScopes is in the unit test U5.
	if _, err := env.Source(t, "noroles-svc").Token(context.Background(), "audit", readScope); err == nil {
		t.Fatal("a client with no role must not get a token with the read scope")
	}

	wrongAudience := env.Token(t, "audit", "identity", "identity:manifest:publish")
	for route := range routes {
		if resp := srv.Do(t, http.MethodGet, route, wrongAudience, nil); resp.Status != http.StatusUnauthorized {
			t.Fatalf("%s with the audience identity: status = %d, want 401", route, resp.Status)
		}
	}
}

// I11: two instances start at the same time on one database. Both are ready.
// Each entry is stored one time.
func TestI11_TwoInstances(t *testing.T) {
	env.ResetAuditStreams(t)
	db := env.NewDB(t)
	results := make(chan *testserver.Server, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			s, err := env.StartE(t, testserver.WithDB(db))
			if err != nil {
				errs <- err
				return
			}
			results <- s
		}()
	}
	for range 2 {
		select {
		case err := <-errs:
			t.Fatalf("an instance does not start: %v", err)
		case s := <-results:
			t.Cleanup(s.Stop)
			s.WaitReady(t)
		}
	}
	p := env.NewProducer(t, "helpdesk")
	const total = 30
	for range total / 5 {
		entries := make([]audit.Entry, 5)
		for i := range entries {
			entries[i] = audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success}
		}
		p.Write(t, entries...)
	}
	direct := pool(t, db)
	waitCount(t, direct, total, selectRows)
	settle()
	if n := count(t, direct, selectRows); n != total {
		t.Fatalf("rows = %d, want %d", n, total)
	}
	if n := count(t, direct, `SELECT count(*) FROM (SELECT entry_id FROM audit_entries GROUP BY service, entry_id HAVING count(*) > 1) d`); n != 0 {
		t.Fatalf("%d entries are stored more than one time", n)
	}
}

// I13: the retention jobs delete the entry and the rejected entry that arrived
// long ago. The entry that arrived now stays, also if its time is old (R5). An
// entry with a time in the future never gets in (R12).
func TestI13_Retention(t *testing.T) {
	var calls atomic.Int32
	fire := make(chan struct{})
	after := func(time.Duration) <-chan time.Time {
		c := make(chan time.Time, 1)
		if calls.Add(1) <= 2 {
			go func() {
				<-fire
				c <- time.Now()
			}()
		}
		return c
	}
	srv := started(t, testserver.WithRetentionAfter(after), testserver.WithConfigure(func(cfg *config.Config) {
		cfg.Retention.AuditEntries.Period = time.Second
		cfg.Retention.RejectedEntries.Period = time.Second
	}))
	db := pool(t, srv.DB)
	insert := `INSERT INTO audit_entries (entry_id, service, time, received_at, action, result, actor, actor_type)
		VALUES (gen_random_uuid(), 'helpdesk', $1, $2, 'helpdesk:ticket:write', 'success', 'usr_1', 'user')`
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	// An old time with a new receipt time stays. A new time with an old receipt
	// time goes.
	for _, ts := range [][2]time.Time{{old, time.Now()}, {time.Now(), old}} {
		if _, err := db.Exec(context.Background(), insert, ts[0], ts[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(context.Background(), `INSERT INTO rejected_entries (stream, stream_id, reason, payload, received_at)
		VALUES ('helpdesk', '1-0', 'invalid_json', '\x7b', '2020-01-01T00:00:00Z'), ('helpdesk', '2-0', 'invalid_json', '\x7b', now() + interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	close(fire)
	waitCount(t, db, 1, selectRows)
	waitCount(t, db, 1, selectRejected)
	if n := count(t, db, `SELECT count(*) FROM audit_entries WHERE time < '2021-01-01'`); n != 1 {
		t.Fatalf("the entry with the old time and the new receipt time is gone")
	}
	if n := count(t, db, `SELECT count(*) FROM rejected_entries WHERE stream_id = '2-0'`); n != 1 {
		t.Fatalf("the new rejected entry is gone")
	}
}

// I14: no log record has a summary, details, an IP address, a user agent, or a
// payload. This is true for a stored entry and for a rejected entry.
func TestI14_NoEntryDataInLogs(t *testing.T) {
	logger, logs := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger))
	p := env.NewProducer(t, "helpdesk")
	secrets := func(_ map[string]any, data map[string]any) {
		data["summary"] = "SECRET-SUMMARY-14"
		data["details"] = map[string]any{"k": "SECRET-DETAILS-14"}
		data["ipAddress"] = "203.0.113.14"
		data["userAgent"] = "SECRET-AGENT-14"
		data["requestId"] = "SECRET-REQUEST-14"
	}
	good := rawEvent(t, "helpdesk", secrets)
	bad := rawEvent(t, "helpdesk", func(e, d map[string]any) {
		secrets(e, d)
		d["result"] = "SECRET-RESULT-14"
	})
	p.PublishRaw(t, good, bad)
	db := pool(t, srv.DB)
	waitCount(t, db, 1, selectRows)
	waitCount(t, db, 1, selectRejected)
	waitNoPending(t, p)
	text := logs.Text()
	for _, secret := range []string{"SECRET-", "203.0.113.14"} {
		if strings.Contains(text, secret) {
			t.Fatalf("a log record has %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, "invalid_result") {
		t.Fatalf("no log record for the rejected entry: %s", text)
	}
}

// I15: an entry with the source /crm on the stream of helpdesk is dropped by
// the consumer before the handler. It is in no table, and the message is
// acknowledged. The consumer metric counts it as invalid.
func TestI15_ForgedSource(t *testing.T) {
	mp, reader := meterOf(t)
	srv := started(t, testserver.WithMeterProvider(mp))
	p := env.NewProducer(t, "helpdesk")
	p.PublishRaw(t, rawEvent(t, "helpdesk", func(e, _ map[string]any) { e["source"] = "/crm" }))
	waitNoPending(t, p)
	deadline := time.Now().Add(20 * time.Second)
	for counter(t, reader, "constellation.events.consumer.handled", "outcome", "invalid") < 1 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := counter(t, reader, "constellation.events.consumer.handled", "outcome", "invalid"); got != 1 {
		t.Fatalf("invalid = %d, want 1", got)
	}
	db := pool(t, srv.DB)
	if a, r := count(t, db, selectRows), count(t, db, selectRejected); a != 0 || r != 0 {
		t.Fatalf("audit_entries = %d, rejected_entries = %d, want 0 and 0", a, r)
	}
}

// I16: an entry with the JSON escape of U+0000 in a details value goes to
// rejected_entries with the reason invalid_text. The valid entry after it is
// stored.
func TestI16_EscapedNUL(t *testing.T) {
	srv := started(t)
	p := env.NewProducer(t, "helpdesk")
	nul := rawEvent(t, "helpdesk", func(_, data map[string]any) { data["details"] = map[string]any{"k": "a\x00b"} })
	if !strings.Contains(string(nul), `\u0000`) {
		t.Fatalf("the payload has no JSON escape of U+0000: %s", nul)
	}
	p.PublishRaw(t, nul, rawEvent(t, "helpdesk", nil))
	db := pool(t, srv.DB)
	waitCount(t, db, 1, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM rejected_entries WHERE reason = 'invalid_text'`)
	waitNoPending(t, p)
}

// I18 (R12): an entry with a time in the future goes to rejected_entries with
// the reason invalid_time. The valid entry after it is stored.
func TestI18_FutureTime(t *testing.T) {
	srv := started(t)
	p := env.NewProducer(t, "helpdesk")
	future := rawEvent(t, "helpdesk", func(e, _ map[string]any) { e["time"] = "9999-12-31T23:59:59Z" })
	p.PublishRaw(t, future, rawEvent(t, "helpdesk", nil))
	db := pool(t, srv.DB)
	waitCount(t, db, 1, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM rejected_entries WHERE reason = 'invalid_time'`)
	waitNoPending(t, p)
}

// I19 (R11, decision D10): the service keeps the intake of a known app that is
// not in audit.apps. The service stores the first entry. After the stop,
// helpdesk writes a second entry, the admin removes helpdesk from audit.apps,
// and a new service on the same database stores the second entry, because
// known_apps in PostgreSQL has helpdesk.
func TestI19_UnregisteredKnownAppAfterRestart(t *testing.T) {
	db, _ := env.NewProxiedDB(t)
	srv := started(t, testserver.WithDB(db))
	p := env.NewProducer(t, "helpdesk")
	p.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	waitCount(t, pool(t, srv.DB), 1, selectRows)
	waitCount(t, pool(t, srv.DB), 1, `SELECT count(*) FROM known_apps WHERE app = 'helpdesk'`)
	srv.Stop()

	p.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	admin := env.ValkeyAdmin(t)
	if err := admin.Do(context.Background(), admin.B().Srem().Key("audit.apps").Member("helpdesk").Build()).Error(); err != nil {
		t.Fatal(err)
	}

	srv2 := env.Start(t, testserver.WithDB(db))
	srv2.WaitReady(t)
	waitCount(t, pool(t, srv2.DB), 2, selectRows)
	waitNoPending(t, p)
	if got := appsMembers(t); len(got) != 0 {
		t.Fatalf("audit.apps = %v, want empty", got)
	}
}

// forgeApps adds forged app names that pass the pattern to the old index
// audit.streams. In v0.9.0 the service does not read this key.
func forgeApps(t *testing.T, n int) {
	t.Helper()
	admin := env.ValkeyAdmin(t)
	for start := 0; start < n; start += 1000 {
		cmd := []string{"SADD", "audit.streams"}
		for i := start; i < min(start+1000, n); i++ {
			cmd = append(cmd, "f"+strconv.Itoa(100000+i))
		}
		if err := admin.Do(context.Background(), admin.B().Arbitrary(cmd...).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

// indexSize returns the size of audit.streams as seen by the admin user.
func indexSize(t *testing.T) int64 {
	t.Helper()
	admin := env.ValkeyAdmin(t)
	n, err := admin.Do(context.Background(), admin.B().Scard().Key("audit.streams").Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// appsMembers returns the members of audit.apps as seen by the admin user.
func appsMembers(t *testing.T) []string {
	t.Helper()
	admin := env.ValkeyAdmin(t)
	m, err := admin.Do(context.Background(), admin.B().Smembers().Key("audit.apps").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(m)
	return m
}

// xinfoStreamCalls reads the number of XINFO STREAM calls that Valkey has seen,
// as admin, from INFO commandstats.
func xinfoStreamCalls(t *testing.T) int64 {
	t.Helper()
	admin := env.ValkeyAdmin(t)
	info, err := admin.Do(context.Background(), admin.B().Info().Section("commandstats").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "cmdstat_xinfo|stream:calls=")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.SplitN(rest, ",", 2)[0], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	return 0
}

// waitCounterAtLeast waits until the counter has at least want for the value.
func waitCounterAtLeast(t *testing.T, reader *sdkmetric.ManualReader, name, key, value string, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for counter(t, reader, name, key, value) < want && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := counter(t, reader, name, key, value); got < want {
		t.Fatalf("%s %s=%s is %d after 30 seconds, want at least %d", name, key, value, got, want)
	}
}

const problems = "constellation.audit.intake.problems"

// I20 (R5): a producer fills the old index audit.streams with 100000 forged
// names. A new registered producer is found, the forged names give no
// XINFO STREAM load, and the service deletes nothing.
func TestI20_ForgedOldIndexGivesNoLoad(t *testing.T) {
	mp, reader := meterOf(t)
	srv := started(t, testserver.WithMeterProvider(mp))
	db := pool(t, srv.DB)
	helpdesk := env.NewProducer(t, "helpdesk")
	helpdesk.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	waitCount(t, db, 1, selectRows)
	forgeApps(t, 100000)
	before := xinfoStreamCalls(t)
	newapp := env.NewProducer(t, "newapp")
	newapp.Write(t, audit.Entry{Action: "newapp:thing:write", Result: audit.Success})
	waitCount(t, db, 2, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM known_apps WHERE app = 'newapp'`)
	third := env.NewProducer(t, "thirdapp")
	third.Write(t, audit.Entry{Action: "thirdapp:thing:write", Result: audit.Success})
	waitCount(t, db, 3, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM known_apps WHERE app = 'thirdapp'`)
	if inc := xinfoStreamCalls(t) - before; inc >= 1000 {
		t.Fatalf("XINFO STREAM calls increased by %d, want less than 1000", inc)
	}
	if got := indexSize(t); got != 100000 {
		t.Fatalf("audit.streams has %d members, want 100000 (no delete)", got)
	}
	for _, reason := range []string{"apps_error", "too_many_apps"} {
		if got := counter(t, reader, problems, "reason", reason); got != 0 {
			t.Fatalf("%s = %d, want 0", reason, got)
		}
	}
}

// I21: a producer that is not registered is not found. After the admin
// registers it, the service stores its entry from the start of the stream.
func TestI21_UnregisteredProducerNotFound(t *testing.T) {
	srv := started(t)
	db := pool(t, srv.DB)
	ghost := env.NewUnregisteredProducer(t, "ghost")
	ghost.Write(t, audit.Entry{Action: "ghost:thing:write", Result: audit.Success})
	newapp := env.NewProducer(t, "newapp")
	newapp.Write(t, audit.Entry{Action: "newapp:thing:write", Result: audit.Success})
	waitCount(t, db, 1, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM known_apps WHERE app = 'newapp'`)
	waitCount(t, db, 0, `SELECT count(*) FROM audit_entries WHERE service = 'ghost'`)
	waitCount(t, db, 0, `SELECT count(*) FROM known_apps WHERE app = 'ghost'`)

	containers.RegisterAuditApps(t, env.ValkeyAdmin(t), "ghost")
	waitCount(t, db, 2, selectRows)
	waitCount(t, db, 1, `SELECT count(*) FROM known_apps WHERE app = 'ghost'`)
}

// I22: the reader user has the v0.8.0 selectors, with no +sscan on audit.apps.
// Each refresh counts apps_error, known_apps keeps the intake of helpdesk, and
// the service does not find the new producer newapp.
func TestI22_ReaderWithOldSelectors(t *testing.T) {
	const v080 = "(~audit.stream:* +xreadgroup +xack +xautoclaim +xpending +xinfo|groups +xinfo|stream +xinfo|consumers +xgroup|create +xgroup|createconsumer +xgroup|delconsumer +xrange) (~audit.streams +sscan +srem)"
	srv := started(t)
	helpdesk := env.NewProducer(t, "helpdesk")
	helpdesk.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	waitCount(t, pool(t, srv.DB), 1, selectRows)
	waitCount(t, pool(t, srv.DB), 1, `SELECT count(*) FROM known_apps WHERE app = 'helpdesk'`)
	srv.Stop()

	mp, reader := meterOf(t)
	srv2 := env.Start(t, testserver.WithDB(srv.DB), testserver.WithMeterProvider(mp), testserver.WithReaderSelectors(v080))
	srv2.WaitReady(t)
	db := pool(t, srv2.DB)
	helpdesk.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	newapp := env.NewProducer(t, "newapp")
	newapp.Write(t, audit.Entry{Action: "newapp:thing:write", Result: audit.Success})
	waitCount(t, db, 2, selectRows)
	waitCounterAtLeast(t, reader, problems, "reason", "apps_error", 2)
	waitCount(t, db, 0, `SELECT count(*) FROM audit_entries WHERE service = 'newapp'`)
	if got := appsMembers(t); !slices.Equal(got, []string{"helpdesk", "newapp"}) {
		t.Fatalf("audit.apps = %v, want [helpdesk newapp]", got)
	}
}

// I23: the reader user has the transition selectors: the selectors of v0.9.0
// and +sscan on audit.streams. The service finds a registered producer and
// counts no apps_error.
func TestI23_ReaderWithTransitionSelectors(t *testing.T) {
	const transition = "(~audit.stream:* +xreadgroup +xack +xautoclaim +xpending +xinfo|groups +xinfo|stream +xinfo|consumers +xgroup|create +xgroup|createconsumer +xgroup|delconsumer +xrange) (~audit.apps +sscan) (~audit.streams +sscan)"
	mp, reader := meterOf(t)
	srv := started(t, testserver.WithMeterProvider(mp), testserver.WithReaderSelectors(transition))
	newapp := env.NewProducer(t, "newapp")
	newapp.Write(t, audit.Entry{Action: "newapp:thing:write", Result: audit.Success})
	waitCount(t, pool(t, srv.DB), 1, selectRows)
	if got := counter(t, reader, problems, "reason", "apps_error"); got != 0 {
		t.Fatalf("apps_error = %d, want 0", got)
	}
}
