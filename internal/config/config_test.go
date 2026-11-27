package config_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-audit/internal/config"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("AUDIT_DATABASE__URL", "postgres://u:p@localhost:5432/audit?sslmode=disable")
	t.Setenv("AUDIT_DATABASE__NAME", "audit")
	t.Setenv("AUDIT_VALKEY__ADDRESS", "localhost:6379")
	t.Setenv("AUDIT_VALKEY__KEY_PREFIX", "audit")
	t.Setenv("AUDIT_AUTH__ISSUER", "https://id.example.com")
	t.Setenv("AUDIT_CLIENT__ID", "audit")
	t.Setenv("AUDIT_CLIENT__KEY_ID", "k1")
	t.Setenv("AUDIT_CLIENT__PRIVATE_KEY", "pem")
	t.Setenv("AUDIT_CLIENT__ISSUER", "https://id.example.com")
	t.Setenv("AUDIT_IDENTITY__URL", "https://id.example.com")
}

// U6: a bad refresh interval, a bad count, and a pool of 2 connections give an
// error that names the variable. The audience is the constant audit, also with
// AUDIT_AUTH__AUDIENCE.
func TestU6_Config(t *testing.T) {
	setRequired(t)
	bad := []struct{ name, value string }{
		{"AUDIT_INTAKE__REFRESH", "4s"},
		{"AUDIT_INTAKE__REFRESH", "6m"},
		{"AUDIT_INTAKE__REFRESH", "abc"},
		{"AUDIT_INTAKE__COUNT", "0"},
		{"AUDIT_INTAKE__COUNT", "1001"},
		{"AUDIT_DATABASE__MAX_CONNS", "2"},
	}
	for _, tc := range bad {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := config.Load()
			if err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("error = %v, want one that names %s", err, tc.name)
			}
		})
	}

	t.Setenv("AUDIT_AUTH__AUDIENCE", "identity")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Auth.JWT().Audience; got != "audit" {
		t.Fatalf("audience = %q, want audit", got)
	}
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Intake.Refresh != 30*time.Second || cfg.Intake.Count != 100 {
		t.Fatalf("intake = %+v", cfg.Intake)
	}
	if cfg.Database.MaxConns < config.MinMaxConns {
		t.Fatalf("max conns = %d", cfg.Database.MaxConns)
	}
	if cfg.Retention.AuditEntries.Period != 0 || config.DefaultRetentionPeriod != 1440*time.Hour {
		t.Fatalf("retention = %+v", cfg.Retention)
	}
}

func TestMissingVariable(t *testing.T) {
	setRequired(t)
	if err := os.Unsetenv("AUDIT_AUTH__ISSUER"); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load()
	if err == nil || !strings.Contains(err.Error(), "AUTH__ISSUER") {
		t.Fatalf("error = %v", err)
	}
}

// U8: the export variables and the pool rule. A bad value gives an error that
// names the variable. MaxConns 8 with concurrency 2 is valid.
func TestU8_ExportConfig(t *testing.T) {
	setRequired(t)
	bad := []struct {
		name  string
		env   map[string]string
		wants string
	}{
		{"max entries 0", map[string]string{"AUDIT_EXPORT__MAX_ENTRIES": "0"}, "AUDIT_EXPORT__MAX_ENTRIES"},
		{"max entries too large", map[string]string{"AUDIT_EXPORT__MAX_ENTRIES": "1000001"}, "AUDIT_EXPORT__MAX_ENTRIES"},
		{"concurrency 17", map[string]string{"AUDIT_EXPORT__CONCURRENCY": "17"}, "AUDIT_EXPORT__CONCURRENCY"},
		{"concurrency 0", map[string]string{"AUDIT_EXPORT__CONCURRENCY": "0"}, "AUDIT_EXPORT__CONCURRENCY"},
		{"timeout 30s", map[string]string{"AUDIT_EXPORT__TIMEOUT": "30s"}, "AUDIT_EXPORT__TIMEOUT"},
		{"timeout 2h", map[string]string{"AUDIT_EXPORT__TIMEOUT": "2h"}, "AUDIT_EXPORT__TIMEOUT"},
		{"MaxConns 7 with concurrency 2", map[string]string{"AUDIT_DATABASE__MAX_CONNS": "7"}, "AUDIT_DATABASE__MAX_CONNS"},
		{"MaxConns 9 with concurrency 3", map[string]string{"AUDIT_DATABASE__MAX_CONNS": "9", "AUDIT_EXPORT__CONCURRENCY": "3"}, "AUDIT_DATABASE__MAX_CONNS"},
		{"bad trusted proxy", map[string]string{"AUDIT_HTTP__TRUSTED_PROXIES": "10.0.0.0/33"}, "AUDIT_HTTP__TRUSTED_PROXIES"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := config.Load()
			if err == nil || !strings.Contains(err.Error(), tc.wants) {
				t.Fatalf("error = %v, want one that names %s", err, tc.wants)
			}
		})
	}

	t.Setenv("AUDIT_DATABASE__MAX_CONNS", "8")
	t.Setenv("AUDIT_HTTP__TRUSTED_PROXIES", "10.0.0.0/8, 192.168.0.0/16")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Export.MaxEntries != 100000 || cfg.Export.Concurrency != 2 || cfg.Export.Timeout != 10*time.Minute {
		t.Fatalf("export defaults = %+v", cfg.Export)
	}
	ci, err := cfg.HTTP.ClientInfo()
	if err != nil || len(ci.TrustedProxies) != 2 {
		t.Fatalf("trusted proxies = %v, err = %v", ci.TrustedProxies, err)
	}
}
