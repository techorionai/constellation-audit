// Package streams is the port to the audit streams of the producers.
package streams

import (
	"context"
	"errors"
	"time"

	platformstreams "github.com/techorionai/constellation-platform-go/streams"
)

var (
	// ErrNoGroup means that the stream has no consumer group of the audit
	// service.
	ErrNoGroup = errors.New("streams: the consumer group does not exist")
	// ErrTooManyApps means that the index has more apps than the limit. The
	// list that comes with the error is usable.
	ErrTooManyApps = errors.New("streams: the audit index has too many apps")
)

// Runner runs one consumer until the context ends.
type Runner interface {
	Run(ctx context.Context) error
}

// Streams finds the producers and makes the consumers.
type Streams interface {
	// Apps returns the sorted apps that write an audit stream. With
	// ErrTooManyApps, it also returns the first apps.
	Apps(ctx context.Context) ([]string, error)
	// Lag returns the age of the oldest entry that the group audit did not
	// acknowledge on the audit stream of app. It returns ErrNoGroup if the
	// group does not exist.
	Lag(ctx context.Context, app string) (time.Duration, error)
	// NewConsumer makes the consumer of the audit stream of app.
	NewConsumer(app string, h platformstreams.Handler) (Runner, error)
}
