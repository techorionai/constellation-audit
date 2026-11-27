package testserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/retention"
	"go.opentelemetry.io/otel/metric"

	"github.com/techorionai/constellation-audit/internal/config"
	"github.com/techorionai/constellation-audit/internal/services"
)

type settings struct {
	db          *DB
	logger      *slog.Logger
	client      string
	configure   []func(*config.Config)
	meter       metric.MeterProvider
	intake      time.Duration
	claimIdle   time.Duration
	retention   func(time.Duration) <-chan time.Time
	plainValkey bool
	readerSel   []string
}

// Option changes a test server.
type Option func(*settings)

// WithDB uses an existing database, for example for a second instance.
func WithDB(db DB) Option { return func(s *settings) { s.db = &db } }

// WithLogger sets the logger of the service, for example a LogCapture.
func WithLogger(l *slog.Logger) Option { return func(s *settings) { s.logger = l } }

// WithMeterProvider sets the meter provider of the intake instruments.
func WithMeterProvider(mp metric.MeterProvider) Option { return func(s *settings) { s.meter = mp } }

// WithIntakeRefresh sets the refresh interval of the intake. The default is
// 500 ms.
func WithIntakeRefresh(d time.Duration) Option { return func(s *settings) { s.intake = d } }

// WithClaimIdle sets the idle time after which a pending message comes again.
// The default is 1 s.
func WithClaimIdle(d time.Duration) Option { return func(s *settings) { s.claimIdle = d } }

// WithRetentionAfter sets the timer function of the retention runner.
func WithRetentionAfter(after func(time.Duration) <-chan time.Time) Option {
	return func(s *settings) { s.retention = after }
}

// WithReaderSelectors makes the Valkey reader user of the service with these
// selectors in place of streamsvalkey.AuditReaderSelectors(). The option has no
// effect with WithPlainValkey. With no argument, the service gets the default
// selectors.
func WithReaderSelectors(selectors ...string) Option {
	return func(s *settings) { s.readerSel = selectors }
}

// WithPlainValkey makes the service connect with the default user of Valkey,
// which has all rights. The default is an ACL user like the user in production.
func WithPlainValkey() Option { return func(s *settings) { s.plainValkey = true } }

// WithConfigure changes the config before the service starts.
func WithConfigure(f func(*config.Config)) Option {
	return func(s *settings) { s.configure = append(s.configure, f) }
}

// Server is a running audit service.
type Server struct {
	App        *services.App
	Config     config.Config
	DB         DB
	ValkeyUser string
	PublicURL  string
	AdminURL   string

	stopOnce sync.Once
}

// Start starts the audit service in the test process. It uses the client
// audit of the identity fixture and a new database. The cleanup of the test
// stops it.
func (e *Env) Start(t testing.TB, opts ...Option) *Server {
	t.Helper()
	srv, err := e.StartE(t, opts...)
	if err != nil {
		t.Fatalf("testserver: start the audit service: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

// StartE is Start with an error result. It is safe to call it from a goroutine
// of a test. The caller stops the server.
func (e *Env) StartE(t testing.TB, opts ...Option) (*Server, error) {
	s := settings{client: "audit", intake: 500 * time.Millisecond, claimIdle: time.Second}
	for _, o := range opts {
		o(&s)
	}
	var db DB
	if s.db != nil {
		db = *s.db
	} else {
		db = e.NewDB(t)
	}
	cfg := config.Config{
		LogLevel: slog.LevelInfo,
		HTTP:     config.HTTP{Addr: "127.0.0.1:0"},
		Database: clientpg.Config{URL: db.URL, Database: db.Name, MaxConns: 10, ConnectTimeout: 5 * time.Second},
		Valkey:   clientvk.Config{Address: e.ValkeyAddress(t), Password: vkPassword, KeyPrefix: "audit", DialTimeout: 5 * time.Second},
		Auth: config.Auth{
			Issuer: e.IssuerURL, RefreshInterval: time.Minute, MinRefetchInterval: time.Second,
			Leeway: 30 * time.Second, HTTPTimeout: 5 * time.Second, AllowInsecureHTTP: true,
		},
		Client: e.ClientConfig(s.client),
		Intake: config.Intake{Refresh: s.intake, Count: config.DefaultIntakeCount},
		Export: config.Export{MaxEntries: 100000, Concurrency: 2, Timeout: 10 * time.Minute},
		Retention: config.Retention{
			AuditEntries:    retention.Config{Interval: time.Hour, BatchSize: retention.DefaultBatchSize},
			RejectedEntries: retention.Config{Interval: time.Hour, BatchSize: retention.DefaultBatchSize},
		},
	}
	valkeyUser := ""
	if !s.plainValkey {
		var pw string
		if s.readerSel != nil {
			valkeyUser, pw = e.aclUser(t, "audit", s.readerSel...)
		} else {
			valkeyUser, pw = e.NewValkeyACLUser(t)
		}
		cfg.Valkey.Username, cfg.Valkey.Password = valkeyUser, platformconfig.Secret(pw)
	}
	cfg.Admin.Addr = "127.0.0.1:0"
	cfg.Admin.CheckTimeout = 2 * time.Second
	cfg.Admin.DrainDelay = time.Millisecond
	cfg.Identity.URL = e.IssuerURL
	cfg.Identity.Timeout = 10 * time.Second
	cfg.Identity.AllowInsecureHTTP = true
	for _, f := range s.configure {
		f(&cfg)
	}
	logger := s.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	opts2 := []services.Option{services.WithLogger(logger), services.WithClaimIdle(s.claimIdle)}
	if s.retention != nil {
		opts2 = append(opts2, services.WithRetentionAfter(s.retention))
	}
	if s.meter != nil {
		opts2 = append(opts2, services.WithMeterProvider(s.meter))
	}
	app, err := services.Start(context.Background(), cfg, opts2...)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		App: app, Config: cfg, DB: db, ValkeyUser: valkeyUser,
		PublicURL: "http://" + app.PublicAddr(), AdminURL: "http://" + app.AdminAddr(),
	}
	return srv, nil
}

// Stop stops the service. It is safe to call it more than one time.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.App.Shutdown(ctx)
	})
}

// Ready returns the status of /readyz.
func (s *Server) Ready() int {
	resp, err := http.Get(s.AdminURL + "/readyz") //nolint:gosec,noctx // The URL is the local admin port of a test.
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// WaitReady waits until /readyz is 200.
func (s *Server) WaitReady(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if s.Ready() == http.StatusOK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("testserver: /readyz is %d after 90 seconds", s.Ready())
}

// Response is the answer of a request.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// JSON decodes the body into v.
func (r Response) JSON(t testing.TB, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("testserver: body %q is not JSON: %v", r.Body, err)
	}
}

// Code returns the code of a Problem body.
func (r Response) Code(t testing.TB) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	r.JSON(t, &p)
	return p.Code
}

// Do sends a request with a bearer token. A token "" sends no header.
func (s *Server) Do(t testing.TB, method, path, token string, body []byte) Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, s.PublicURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("testserver: %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

// LogCapture is a slog handler that keeps the records as text.
type LogCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// NewLogCapture makes a logger that writes JSON records to the capture.
func NewLogCapture() (*slog.Logger, *LogCapture) {
	c := &LogCapture{}
	return slog.New(slog.NewJSONHandler(&lockedWriter{c}, &slog.HandlerOptions{Level: slog.LevelDebug})), c
}

type lockedWriter struct{ c *LogCapture }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.buf.Write(p)
}

// Text returns all records.
func (c *LogCapture) Text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// Records returns the records that have the given action.
func (c *LogCapture) Records(action string) []map[string]any {
	var out []map[string]any
	for _, line := range strings.Split(c.Text(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["action"] == action {
			out = append(out, m)
		}
	}
	return out
}

// Open sends a GET request with a bearer token and the headers. It returns
// the response when the headers arrive, before the service sends the body. The
// caller must close the body. A token "" sends no Authorization header.
func (s *Server) Open(t testing.TB, path, token string, header map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.PublicURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	// A transport of its own, so that the response of a test does not share a
	// connection with another request.
	tr := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(tr.CloseIdleConnections)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("testserver: GET %s: %v", path, err)
	}
	return resp
}

// DoHeader is Do for a GET request with more headers.
func (s *Server) DoHeader(t testing.TB, path, token string, header map[string]string) Response {
	t.Helper()
	resp := s.Open(t, path, token, header)
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}
