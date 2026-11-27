package intake_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/executor"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/handlers/worker/intake"
	"github.com/techorionai/constellation-audit/internal/ports/streams"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type fixture struct {
	sup    *intake.Supervisor
	reader *sdkmetric.ManualReader
	logs   *syncBuffer
}

func newFixture(t *testing.T, st *mock.Streams) *fixture {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	logs := &syncBuffer{}
	record := executor.FuncWithReturn[commands.RecordEntry, commands.Outcome](
		func(context.Context, commands.RecordEntry) (commands.Outcome, error) {
			return commands.OutcomeStored, nil
		})
	sup, err := intake.New(intake.Config{
		Streams: st, Record: record, Refresh: intake.MinRefresh,
		Meter: mp.Meter("test"), Log: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{sup: sup, reader: reader, logs: logs}
}

// run starts the refresh loop of the supervisor and stops it at cleanup.
func (f *fixture) refresh(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	go func() {
		defer close(done)
		_ = f.sup.Run(ctx)
	}()
	return ctx
}

func (f *fixture) collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func (f *fixture) counter(t *testing.T, name, attr, value string) int64 {
	t.Helper()
	for _, sm := range f.collect(t).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value("reason"); attr == "reason" && ok && v.AsString() == value {
					return dp.Value
				}
			}
		}
	}
	return 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", what)
}

// U7: the supervisor starts one consumer for a, then one for b. An error of the
// list is counted and logged. No consumer stops.
func TestU7_Supervisor(t *testing.T) {
	var mu sync.Mutex
	call := 0
	st := &mock.Streams{AppsFunc: func(context.Context) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		call++
		switch call {
		case 1:
			return []string{"a"}, nil
		case 2:
			return []string{"a", "b"}, nil
		case 3:
			return nil, errors.New("valkey down")
		default:
			return []string{"a", "b"}, nil
		}
	}}
	f := newFixture(t, st)
	ctx := f.refresh(t)
	_ = ctx
	waitFor(t, "consumer for a", func() bool { return slices.Equal(st.Consumers(), []string{"a"}) })

	f.sup.Refresh(ctx)
	if got := st.Consumers(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("consumers = %v, want [a b]", got)
	}
	f.sup.Refresh(ctx)
	if got := f.counter(t, "constellation.audit.intake.problems", "reason", "apps_error"); got != 1 {
		t.Fatalf("apps_error count = %d, want 1", got)
	}
	if !strings.Contains(f.logs.String(), "cannot read the list of producers") {
		t.Fatalf("logs = %s", f.logs.String())
	}
	if got := f.sup.Started(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("started = %v, a consumer must not stop", got)
	}
	f.sup.Refresh(ctx)
	if got := st.Consumers(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("consumers = %v, want no new consumer", got)
	}
}

func apps(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%s%04d", prefix, i))
	}
	return out
}

// U10: 1000 apps with ErrTooManyApps give 1000 consumers, one warn record that
// names the limit, and one counted event.
func TestU10_TooManyApps(t *testing.T) {
	list := apps("app", 1000)
	st := &mock.Streams{AppsFunc: func(context.Context) ([]string, error) {
		return list, fmt.Errorf("%w: more", streams.ErrTooManyApps)
	}}
	f := newFixture(t, st)
	f.sup.Refresh(context.Background())
	if got := len(st.Consumers()); got != 1000 {
		t.Fatalf("consumers = %d, want 1000", got)
	}
	if n := strings.Count(f.logs.String(), "level=WARN"); n != 1 || !strings.Contains(f.logs.String(), "limit=1000") {
		t.Fatalf("warn records = %d, logs = %s", n, f.logs.String())
	}
	if got := f.counter(t, "constellation.audit.intake.problems", "reason", "too_many_apps"); got != 1 {
		t.Fatalf("too_many_apps count = %d, want 1", got)
	}
}

// U11: after 1000 consumers, a list of 1000 other apps with ErrTooManyApps adds
// no consumer, and the supervisor logs a warn record that names the limit.
func TestU11_ConsumerCap(t *testing.T) {
	first, second := apps("a", 1000), apps("b", 1000)
	var mu sync.Mutex
	call := 0
	st := &mock.Streams{AppsFunc: func(context.Context) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		call++
		if call == 1 {
			return first, nil
		}
		return second, fmt.Errorf("%w: more", streams.ErrTooManyApps)
	}}
	f := newFixture(t, st)
	f.sup.Refresh(context.Background())
	f.sup.Refresh(context.Background())
	if got := len(st.Consumers()); got != 1000 {
		t.Fatalf("consumers = %d, want 1000", got)
	}
	if !strings.Contains(f.logs.String(), "maximum number of consumers") || !strings.Contains(f.logs.String(), "limit=1000") {
		t.Fatalf("logs = %s", f.logs.String())
	}
}

func TestLagGauge(t *testing.T) {
	st := &mock.Streams{
		AppsFunc: func(context.Context) ([]string, error) { return []string{"crm", "helpdesk"}, nil },
		LagFunc: func(_ context.Context, app string) (time.Duration, error) {
			if app == "crm" {
				return 0, streams.ErrNoGroup
			}
			return 3 * time.Second, nil
		},
	}
	f := newFixture(t, st)
	f.sup.Refresh(context.Background())
	got := map[string]float64{}
	for _, sm := range f.collect(t).ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "constellation.audit.intake.lag" {
				for _, dp := range g.DataPoints {
					v, _ := dp.Attributes.Value("stream")
					got[v.AsString()] = dp.Value
				}
			}
		}
	}
	if len(got) != 1 || got["helpdesk"] != 3 {
		t.Fatalf("gauge = %v, want helpdesk=3 only", got)
	}
	if !strings.Contains(f.logs.String(), "no consumer group") || !strings.Contains(f.logs.String(), "stream=crm") {
		t.Fatalf("logs = %s", f.logs.String())
	}
}

func TestNewChecks(t *testing.T) {
	_, err := intake.New(intake.Config{})
	if err == nil {
		t.Fatal("an empty config must be an error")
	}
}
