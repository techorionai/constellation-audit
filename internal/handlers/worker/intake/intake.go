// Package intake holds the worker that reads the audit streams of the producers.
// It is a primary adapter. It calls the application layer only.
package intake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/techorionai/constellation-platform-go/executor"
	platformstreams "github.com/techorionai/constellation-platform-go/streams"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/internal/ports/streams"
)

// MaxConsumers is the maximum number of consumers, one for each producer.
const MaxConsumers = 1000

// Limits of the refresh interval.
const (
	DefaultRefresh = 30 * time.Second
	MinRefresh     = 5 * time.Second
	MaxRefresh     = 5 * time.Minute
)

// LagTimeout is the time limit of one read of the lag of a stream.
const LagTimeout = 5 * time.Second

// Values of the attribute outcome of the counter of entries.
const outcomeError = "error"

// Values of the attribute reason of the counter of refresh problems.
const (
	reasonTooManyApps     = "too_many_apps"
	reasonAppsError       = "apps_error"
	reasonConsumerError   = "consumer_error"
	reasonConsumerStopped = "consumer_stopped"
	reasonKnownAppsError  = "known_apps_error"
)

// Config is the config of the supervisor.
type Config struct {
	// Streams finds the producers and makes the consumers.
	Streams streams.Streams
	// Known keeps the apps that the intake found. The supervisor starts the
	// consumers of the known apps before it reads the index of the producers.
	Known entries.KnownApps
	// Record stores one message.
	Record executor.ExecutorWithReturn[commands.RecordEntry, commands.Outcome]
	// Refresh is the time between two reads of the list of producers.
	Refresh time.Duration
	// Meter makes the instruments.
	Meter metric.Meter
	Log   *slog.Logger
}

// Supervisor starts one consumer for each producer that writes an audit
// stream, and reports the lag of each stream.
type Supervisor struct {
	cfg      Config
	log      *slog.Logger
	entries  metric.Int64Counter
	problems metric.Int64Counter

	mu      sync.Mutex
	started map[string]struct{}
	lags    map[string]float64
	wg      sync.WaitGroup
}

// New makes the supervisor and its instruments.
func New(cfg Config) (*Supervisor, error) {
	if cfg.Streams == nil || cfg.Known == nil || cfg.Record == nil || cfg.Meter == nil {
		return nil, errors.New("intake: the streams, the known apps, the command, and the meter are required")
	}
	if cfg.Refresh <= 0 {
		return nil, errors.New("intake: the refresh interval must be more than 0")
	}
	s := &Supervisor{cfg: cfg, log: cfg.Log, started: map[string]struct{}{}, lags: map[string]float64{}}
	if s.log == nil {
		s.log = slog.Default()
	}
	var err error
	if s.entries, err = cfg.Meter.Int64Counter("constellation.audit.intake.entries",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Number of audit stream messages that the intake handled, by outcome.")); err != nil {
		return nil, fmt.Errorf("intake: entries counter: %w", err)
	}
	if s.problems, err = cfg.Meter.Int64Counter("constellation.audit.intake.problems",
		metric.WithUnit("{event}"),
		metric.WithDescription("Number of problems of the intake supervisor, by reason.")); err != nil {
		return nil, fmt.Errorf("intake: problems counter: %w", err)
	}
	if _, err = cfg.Meter.Float64ObservableGauge("constellation.audit.intake.lag",
		metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest audit entry that the intake did not acknowledge, by stream."),
		metric.WithFloat64Callback(s.observeLag)); err != nil {
		return nil, fmt.Errorf("intake: lag gauge: %w", err)
	}
	return s, nil
}

func (s *Supervisor) observeLag(_ context.Context, o metric.Float64Observer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for app, v := range s.lags {
		o.Observe(v, metric.WithAttributes(attribute.String("stream", app)))
	}
	return nil
}

// Started returns the sorted apps that have a consumer.
func (s *Supervisor) Started() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	apps := make([]string, 0, len(s.started))
	for app := range s.started {
		apps = append(apps, app)
	}
	slices.Sort(apps)
	return apps
}

// Run refreshes the list of producers at the start and each interval. It
// returns nil after the context ends and all consumers have stopped.
func (s *Supervisor) Run(ctx context.Context) error {
	defer s.wg.Wait()
	t := time.NewTicker(s.cfg.Refresh)
	defer t.Stop()
	for {
		s.Refresh(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Refresh starts a consumer for each known app, reads the producers, starts a
// consumer for each new producer and keeps it as a known app, and reads the lag
// of each stream. The known apps come from the store, so a producer that fills
// the index of the producers with forged names cannot push out the real apps
// (R11). The consumers run until ctx ends.
func (s *Supervisor) Refresh(ctx context.Context) {
	known, err := s.cfg.Known.KnownApps(ctx, MaxConsumers)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.problem(ctx, reasonKnownAppsError)
		s.log.ErrorContext(ctx, "the intake cannot read the known apps", "error", err)
	}
	s.startNew(ctx, known)
	apps, err := s.cfg.Streams.Apps(ctx)
	switch {
	case errors.Is(err, streams.ErrTooManyApps):
		s.problem(ctx, reasonTooManyApps)
		s.log.WarnContext(ctx, "the audit index has more producers than the limit, the intake reads the first producers only",
			"limit", MaxConsumers)
	case err != nil:
		if ctx.Err() != nil {
			return
		}
		s.problem(ctx, reasonAppsError)
		s.log.ErrorContext(ctx, "the intake cannot read the list of producers", "error", err)
		return
	}
	s.startNew(ctx, apps)
	s.remember(ctx, apps, known)
	s.collectLag(ctx)
}

// remember keeps the apps that have a consumer and are not known yet. A failure
// is tried again at the next refresh.
func (s *Supervisor) remember(ctx context.Context, apps, known []string) {
	for _, app := range apps {
		if slices.Contains(known, app) || !slices.Contains(s.Started(), app) {
			continue
		}
		if err := s.cfg.Known.RememberApp(ctx, app); err != nil {
			if ctx.Err() == nil {
				s.problem(ctx, reasonKnownAppsError)
				s.log.ErrorContext(ctx, "the intake cannot keep a known app", "stream", app, "error", err)
			}
			return
		}
	}
}

func (s *Supervisor) problem(ctx context.Context, reason string) {
	s.problems.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (s *Supervisor) startNew(ctx context.Context, apps []string) {
	for _, app := range apps {
		s.mu.Lock()
		_, known := s.started[app]
		full := len(s.started) >= MaxConsumers
		s.mu.Unlock()
		if known {
			continue
		}
		if full {
			s.log.WarnContext(ctx, "the intake has the maximum number of consumers, it does not read more producers",
				"limit", MaxConsumers)
			return
		}
		runner, err := s.cfg.Streams.NewConsumer(app, s.handler(app))
		if err != nil {
			s.problem(ctx, reasonConsumerError)
			s.log.ErrorContext(ctx, "the intake cannot make a consumer", "stream", app, "error", err)
			continue
		}
		s.mu.Lock()
		s.started[app] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
				// Forget the app, so that the next refresh starts a new
				// consumer (R4).
				s.mu.Lock()
				delete(s.started, app)
				s.mu.Unlock()
				s.problem(ctx, reasonConsumerStopped)
				s.log.ErrorContext(ctx, "an intake consumer stopped", "stream", app, "error", err)
			}
		}()
	}
}

func (s *Supervisor) collectLag(ctx context.Context) {
	for _, app := range s.Started() {
		if ctx.Err() != nil {
			return
		}
		lctx, cancel := context.WithTimeout(ctx, LagTimeout)
		d, err := s.cfg.Streams.Lag(lctx, app)
		cancel()
		switch {
		case errors.Is(err, streams.ErrNoGroup):
			s.mu.Lock()
			delete(s.lags, app)
			s.mu.Unlock()
			s.log.WarnContext(ctx, "the audit stream has no consumer group of the audit service", "stream", app)
		case err != nil:
			// An old value is wrong now, so the gauge has no value (R8).
			s.mu.Lock()
			delete(s.lags, app)
			s.mu.Unlock()
			if ctx.Err() == nil {
				s.log.WarnContext(ctx, "the intake cannot read the lag of a stream", "stream", app, "error", err)
			}
		default:
			s.mu.Lock()
			s.lags[app] = d.Seconds()
			s.mu.Unlock()
		}
	}
}

// handler returns the handler of the messages of one stream. An error leaves
// the message pending, and the consumer delivers it again.
func (s *Supervisor) handler(app string) platformstreams.Handler {
	return func(ctx context.Context, m platformstreams.Message) error {
		outcome, err := s.cfg.Record.Execute(ctx, commands.RecordEntry{Stream: app, StreamID: m.ID, Payload: m.Event})
		if err != nil {
			s.count(ctx, outcomeError)
			return err
		}
		s.count(ctx, string(outcome))
		return nil
	}
}

func (s *Supervisor) count(ctx context.Context, outcome string) {
	s.entries.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
