// Package config holds the configuration of the audit service. The values come
// from environment variables with the prefix AUDIT_.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/techorionai/constellation-platform-go/auth/clientcred"
	"github.com/techorionai/constellation-platform-go/auth/jwt"
	"github.com/techorionai/constellation-platform-go/clients/identity"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/health"
	"github.com/techorionai/constellation-platform-go/retention"
)

// Prefix is the prefix of all environment variables of the service.
const Prefix = "AUDIT_"

// Audience is the audience of the access tokens that the service accepts. The
// operator cannot change it.
const Audience = "audit"

// MinMaxConns is the smallest size of the database pool. The retention runner
// needs 2 connections for each tick, and the intake and the API need others.
const MinMaxConns = 4

// DefaultRetentionPeriod is the age after which the retention jobs delete a
// row: 60 days.
const DefaultRetentionPeriod = 60 * 24 * time.Hour

// Limits of the intake.
const (
	DefaultIntakeRefresh = 30 * time.Second
	MinIntakeRefresh     = 5 * time.Second
	MaxIntakeRefresh     = 5 * time.Minute
	DefaultIntakeCount   = 100
	MinIntakeCount       = 1
	MaxIntakeCount       = 1000
)

// HTTP is the config of the public listener.
type HTTP struct {
	Addr string `env:"ADDR" envDefault:":8080"`
}

// Auth is the config of the token verifier. It has the fields of
// auth/jwt.Config except Audience. The audience is the constant Audience.
type Auth struct {
	Issuer             string        `env:"ISSUER,required"`
	JWKSURL            string        `env:"JWKS_URL"`
	RefreshInterval    time.Duration `env:"REFRESH_INTERVAL" envDefault:"10m"`
	MinRefetchInterval time.Duration `env:"MIN_REFETCH_INTERVAL" envDefault:"30s"`
	Leeway             time.Duration `env:"LEEWAY" envDefault:"30s"`
	HTTPTimeout        time.Duration `env:"HTTP_TIMEOUT" envDefault:"5s"`
	AllowInsecureHTTP  bool          `env:"ALLOW_INSECURE_HTTP" envDefault:"false"`
}

// JWT returns the config of the verifier, with the audience of the service.
func (a Auth) JWT() jwt.Config {
	return jwt.Config{
		Issuer:             a.Issuer,
		Audience:           Audience,
		JWKSURL:            a.JWKSURL,
		RefreshInterval:    a.RefreshInterval,
		MinRefetchInterval: a.MinRefetchInterval,
		Leeway:             a.Leeway,
		HTTPTimeout:        a.HTTPTimeout,
		AllowInsecureHTTP:  a.AllowInsecureHTTP,
	}
}

// Intake is the config of the intake of the audit streams.
type Intake struct {
	// Refresh is the time between two reads of the list of producers.
	Refresh time.Duration `env:"REFRESH" envDefault:"30s"`
	// Count is the maximum number of messages of one read of a stream.
	Count int `env:"COUNT" envDefault:"100"`
}

// Retention is the config of the retention jobs. A job deletes the rows that
// are older than the period, 60 days by default.
type Retention struct {
	AuditEntries    retention.Config `envPrefix:"AUDIT_ENTRIES__"`
	RejectedEntries retention.Config `envPrefix:"REJECTED_ENTRIES__"`
}

// Config is the configuration of the service.
type Config struct {
	LogLevel  slog.Level        `env:"LOG_LEVEL" envDefault:"INFO"`
	HTTP      HTTP              `envPrefix:"HTTP__"`
	Admin     health.Config     `envPrefix:"ADMIN__"`
	Database  clientpg.Config   `envPrefix:"DATABASE__"`
	Valkey    clientvk.Config   `envPrefix:"VALKEY__"`
	Auth      Auth              `envPrefix:"AUTH__"`
	Client    clientcred.Config `envPrefix:"CLIENT__"`
	Identity  identity.Config   `envPrefix:"IDENTITY__"`
	Intake    Intake            `envPrefix:"INTAKE__"`
	Retention Retention         `envPrefix:"RETENTION__"`
}

// Load reads the environment and checks the values.
func Load() (Config, error) {
	var cfg Config
	if err := platformconfig.Load(Prefix, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the values that the loader cannot check.
func (c Config) Validate() error {
	var errs []error
	if c.Intake.Refresh < MinIntakeRefresh || c.Intake.Refresh > MaxIntakeRefresh {
		errs = append(errs, fmt.Errorf("%sINTAKE__REFRESH: must be from %s to %s", Prefix, MinIntakeRefresh, MaxIntakeRefresh))
	}
	if c.Intake.Count < MinIntakeCount || c.Intake.Count > MaxIntakeCount {
		errs = append(errs, fmt.Errorf("%sINTAKE__COUNT: must be from %d to %d", Prefix, MinIntakeCount, MaxIntakeCount))
	}
	if c.Database.MaxConns != 0 && c.Database.MaxConns < MinMaxConns {
		errs = append(errs, fmt.Errorf("%sDATABASE__MAX_CONNS: must be %d or more", Prefix, MinMaxConns))
	}
	return errors.Join(errs...)
}
