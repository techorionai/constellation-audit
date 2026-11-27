// Package valkey is the Valkey adapter of the audit streams port.
package valkey

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	platformstreams "github.com/techorionai/constellation-platform-go/streams"
	streamsvalkey "github.com/techorionai/constellation-platform-go/streams/valkey"
	valkeygo "github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/metric"

	"github.com/techorionai/constellation-audit/internal/ports/streams"
)

// GroupApp is the app of the consumer. The group has the same name.
const GroupApp = "audit"

// Streams reads the audit streams with the helpers of platform-go. The client
// is the raw valkey-go client. The stream keys do not use the key prefix of the
// service.
type Streams struct {
	c     valkeygo.Client
	count int
	idle  time.Duration
	log   *slog.Logger
	meter metric.MeterProvider
}

var _ streams.Streams = (*Streams)(nil)

// NewStreams makes the adapter. count is the maximum number of messages of one
// read. claimIdle is the idle time after which a pending message comes
// again. 0 means the default of platform-go. A nil meter provider means the
// global one.
func NewStreams(c valkeygo.Client, count int, claimIdle time.Duration, log *slog.Logger, mp metric.MeterProvider) *Streams {
	return &Streams{c: c, count: count, idle: claimIdle, log: log, meter: mp}
}

// Apps returns the sorted apps with an audit stream.
func (s *Streams) Apps(ctx context.Context) ([]string, error) {
	apps, err := streamsvalkey.AuditApps(ctx, s.c)
	if errors.Is(err, streamsvalkey.ErrTooManyAuditApps) {
		return apps, fmt.Errorf("%w: %w", streams.ErrTooManyApps, err)
	}
	return apps, err
}

// Lag returns the age of the oldest entry that the group did not acknowledge.
func (s *Streams) Lag(ctx context.Context, app string) (time.Duration, error) {
	d, err := streamsvalkey.LagFor(ctx, s.c, platformstreams.Audit, app, GroupApp)
	if errors.Is(err, streamsvalkey.ErrNoGroup) {
		return 0, streams.ErrNoGroup
	}
	return d, err
}

// NewConsumer makes the consumer of the audit stream of app. The consumer never
// drops a message that the handler refuses.
func (s *Streams) NewConsumer(app string, h platformstreams.Handler) (streams.Runner, error) {
	g, err := streamsvalkey.NewGroup(s.c, streamsvalkey.GroupConfig{
		App: GroupApp, Stream: app, Family: platformstreams.Audit, Group: GroupApp, StartID: "0",
	})
	if err != nil {
		return nil, fmt.Errorf("make the group for %s: %w", app, err)
	}
	c, err := platformstreams.NewConsumer(g, h, platformstreams.ConsumerOptions{
		Count: s.count, ClaimIdle: s.idle, NeverDrop: true, Logger: s.log, MeterProvider: s.meter,
	})
	if err != nil {
		return nil, fmt.Errorf("make the consumer for %s: %w", app, err)
	}
	return c, nil
}
