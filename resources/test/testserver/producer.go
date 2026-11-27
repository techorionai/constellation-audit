package testserver

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/audit"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/resources/test/containers"
	"github.com/techorionai/constellation-platform-go/resources/test/mock"
	"github.com/techorionai/constellation-platform-go/streams"
	streamsvalkey "github.com/techorionai/constellation-platform-go/streams/valkey"
	valkeygo "github.com/valkey-io/valkey-go"
)

var selectorPattern = regexp.MustCompile(`\([^)]*\)`)

// aclUser makes an ACL user with the key pattern ~<prefix>:*, the command rule
// of platform-go, and the selectors. It returns the name and the password.
func (e *Env) aclUser(t testing.TB, prefix string, selectors ...string) (string, string) {
	t.Helper()
	e.mu.Lock()
	e.dbSeq++
	n := e.dbSeq
	e.mu.Unlock()
	user := fmt.Sprintf("%s_acl_%d_%d", prefix, time.Now().UnixNano(), n)
	password := fmt.Sprintf("pw%d", time.Now().UnixNano())
	args := []string{"ACL", "SETUSER", user, "on", ">" + password, "~" + prefix + ":*"}
	args = append(args, strings.Fields(containers.ACLRule)...)
	for _, s := range selectors {
		args = append(args, selectorPattern.FindAllString(s, -1)...)
	}
	admin := e.ValkeyAdmin(t)
	if err := admin.Do(context.Background(), admin.B().Arbitrary(args[0]).Args(args[1:]...).Build()).Error(); err != nil {
		t.Fatalf("testserver: ACL SETUSER: %v", err)
	}
	return user, password
}

// NewValkeyACLUser makes an ACL user like the user of the audit service in
// production. It has the key pattern ~audit:*, the command rule of
// platform-go, and the selectors of streamsvalkey.AuditReaderSelectors. It
// returns the name and the password.
func (e *Env) NewValkeyACLUser(t testing.TB) (string, string) {
	t.Helper()
	return e.aclUser(t, "audit", streamsvalkey.AuditReaderSelectors())
}

// Producer is a service that writes audit entries. It has its own Valkey ACL
// user like the user of a producer in production: the selectors of the event
// streams and the selectors of an audit producer. It writes entries with the
// audit writer of platform-go.
type Producer struct {
	// App is the app key of the producer.
	App string

	client valkeygo.Client
	pub    *streamsvalkey.Publisher
	ins    *mock.AuditInserter
	writer audit.Writer
}

// NewProducer makes a producer for an app. The ACL user and the client end at
// the cleanup of the test.
func (e *Env) NewProducer(t testing.TB, app string) *Producer {
	t.Helper()
	user, pw := e.aclUser(t, app, streamsvalkey.ACLSelectors(app), streamsvalkey.AuditProducerSelectors(app))
	c, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{e.ValkeyAddress(t)}, Username: user, Password: pw, DisableCache: true,
	})
	if err != nil {
		t.Fatalf("testserver: producer client: %v", err)
	}
	t.Cleanup(c.Close)
	ins := &mock.AuditInserter{}
	w, err := audit.New(ins, audit.Config{App: app, ClientID: app + "-svc"})
	if err != nil {
		t.Fatal(err)
	}
	return &Producer{
		App: app, client: c, ins: ins, writer: w,
		pub: streamsvalkey.NewPublisher(c, app, streamsvalkey.WithFamily(streams.Audit)),
	}
}

// Write writes entries with the audit writer and publishes them to the audit
// stream, as the relay does. It returns the IDs of the entries.
func (p *Producer) Write(t testing.TB, entries ...audit.Entry) []string {
	t.Helper()
	before := len(p.ins.Rows())
	if err := p.writer.Write(context.Background(), entries...); err != nil {
		t.Fatalf("testserver: audit writer: %v", err)
	}
	rows := p.ins.Rows()[before:]
	payloads := make([][]byte, 0, len(rows))
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		payloads = append(payloads, r.Payload)
		ids = append(ids, r.EventID)
	}
	p.PublishRaw(t, payloads...)
	return ids
}

// PublishRaw adds payloads to the audit stream of the producer with no check.
// A test uses it for entries that the writer refuses.
func (p *Producer) PublishRaw(t testing.TB, payloads ...[]byte) {
	t.Helper()
	if err := p.pub.Publish(context.Background(), payloads); err != nil {
		t.Fatalf("testserver: publish: %v", err)
	}
}

// StreamKey returns the key of the audit stream of the producer.
func (p *Producer) StreamKey() string { return streams.KeyFor(streams.Audit, p.App) }

// NewProxiedDB starts a PostgreSQL container that only one test uses, and
// returns a database in it with a TCP proxy. The URL of the database has the
// address of the proxy. Stop the proxy to make the database unavailable, and
// start it again to bring the database back.
func (e *Env) NewProxiedDB(t testing.TB) (DB, *TCPProxy) {
	t.Helper()
	db, _ := e.NewIsolatedDB(t)
	u, err := url.Parse(string(db.URL))
	if err != nil {
		t.Fatalf("testserver: database URL: %v", err)
	}
	proxy := NewTCPProxy(t, u.Host)
	u.Host = proxy.Addr()
	return DB{URL: platformconfig.Secret(u.String()), Name: db.Name}, proxy
}

// ResetAuditStreams deletes the audit streams and the audit index of all
// producers. A test calls it at its start, so that the tests of the package do
// not read the entries of each other.
func (e *Env) ResetAuditStreams(t testing.TB) {
	t.Helper()
	admin := e.ValkeyAdmin(t)
	ctx := context.Background()
	keys := []string{streams.AuditIndexKey}
	var cursor uint64
	for {
		entry, err := admin.Do(ctx, admin.B().Scan().Cursor(cursor).Match(streams.AuditKeyPrefix+"*").Count(1000).Build()).AsScanEntry()
		if err != nil {
			t.Fatalf("testserver: scan audit streams: %v", err)
		}
		keys = append(keys, entry.Elements...)
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	if err := admin.Do(ctx, admin.B().Del().Key(keys...).Build()).Error(); err != nil {
		t.Fatalf("testserver: delete audit streams: %v", err)
	}
}

// ValkeyClient makes a client of Valkey for an ACL user. It closes the client at
// the cleanup of the test.
func (e *Env) ValkeyClient(t testing.TB, user, password string) valkeygo.Client {
	t.Helper()
	c, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{e.ValkeyAddress(t)}, Username: user, Password: password, DisableCache: true,
	})
	if err != nil {
		t.Fatalf("testserver: valkey client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}
