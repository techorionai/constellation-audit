//go:build integration

package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/techorionai/constellation-audit/resources/test/testserver"
)

// env is the shared containers of the package.
var env *testserver.Env

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	e, err := testserver.NewEnv(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "the test environment does not start: %v\n", err)
		os.Exit(1)
	}
	env = e
	code := m.Run()
	env.Close()
	os.Exit(code)
}

const readScope = "audit:log:read"

// started clears the audit streams, starts a service, and waits until it is
// ready. The cleanup of the test stops the service.
func started(t *testing.T, opts ...testserver.Option) *testserver.Server {
	t.Helper()
	env.ResetAuditStreams(t)
	srv := env.Start(t, opts...)
	srv.WaitReady(t)
	return srv
}

// meterOf makes a meter provider with a reader, for the counters of the
// service.
func meterOf(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// counter returns the value of the counter with the attribute.
func counter(t *testing.T, reader *sdkmetric.ManualReader, name, key, value string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(key)); ok && v.AsString() == value {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// gaugeByStream returns the values of the lag gauge by the attribute stream.
func gaugeByStream(t *testing.T, reader *sdkmetric.ManualReader) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "constellation.audit.intake.lag" {
				for _, dp := range g.DataPoints {
					v, _ := dp.Attributes.Value("stream")
					out[v.AsString()] = dp.Value
				}
			}
		}
	}
	return out
}

// pool connects to the database of a server as its service user.
func pool(t *testing.T, db testserver.DB) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), string(db.URL))
	if err != nil {
		t.Fatalf("connect to the database: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// count runs a query that returns one number.
func count(t *testing.T, p *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := p.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// waitCount waits until the query gives want.
func waitCount(t *testing.T, p *pgxpool.Pool, want int, sql string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	got := -1
	for time.Now().Before(deadline) {
		var n int
		if err := p.QueryRow(context.Background(), sql, args...).Scan(&n); err == nil {
			got = n
			if n == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s: count = %d after 30 seconds, want %d", sql, got, want)
}

// settle waits a short time so that a count that must stay the same has the
// time to change.
func settle() { time.Sleep(2 * time.Second) }

// rawEvent makes the JSON of a CloudEvent for the app. A change function can
// break it.
func rawEvent(t *testing.T, app string, change func(env, data map[string]any)) []byte {
	t.Helper()
	data := map[string]any{"action": app + ":ticket:write", "result": "success", "summary": "Ticket changed"}
	envelope := map[string]any{
		"specversion": "1.0", "id": uuid.NewString(), "source": "/" + app, "type": app + ".audit.recorded",
		"time": time.Now().UTC().Format(time.RFC3339Nano), "datacontenttype": "application/json",
		"actor": "usr_1", "actortype": "user",
	}
	if change != nil {
		change(envelope, data)
	}
	envelope["data"] = data
	b, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// pending returns the number of pending messages of the group audit.
func pending(t *testing.T, admin valkeygo.Client, p *testserver.Producer) int64 {
	t.Helper()
	res, err := admin.Do(context.Background(), admin.B().Arbitrary("XPENDING", p.StreamKey(), "audit").Build()).ToArray()
	if err != nil || len(res) == 0 {
		t.Fatalf("XPENDING: %v", err)
	}
	n, err := res[0].AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// waitPending waits until the group audit has no pending message and has read
// the messages.
func waitNoPending(t *testing.T, p *testserver.Producer) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	admin := env.ValkeyAdmin(t)
	for time.Now().Before(deadline) {
		res, err := admin.Do(context.Background(), admin.B().Arbitrary("XINFO", "GROUPS", p.StreamKey()).Build()).ToArray()
		if err == nil && len(res) > 0 {
			m, merr := res[0].AsMap()
			if merr == nil {
				lagMsg := m["lag"]
				lag, lerr := lagMsg.AsInt64()
				if lerr == nil && pending(t, admin, p) == 0 && lag == 0 {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the group audit still has pending or unread messages on %s", p.StreamKey())
}
