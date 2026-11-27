//go:build integration

package services_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/audit"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	"github.com/techorionai/constellation-platform-go/executor"
	platformstreams "github.com/techorionai/constellation-platform-go/streams"

	"github.com/techorionai/constellation-audit/internal/adapters/postgres"
	vkadapter "github.com/techorionai/constellation-audit/internal/adapters/valkey"
	"github.com/techorionai/constellation-audit/internal/app/commands"
	"github.com/techorionai/constellation-audit/internal/handlers/worker/intake"
	"github.com/techorionai/constellation-audit/internal/ports/streams"
	"github.com/techorionai/constellation-audit/internal/services"
	"github.com/techorionai/constellation-audit/resources/test/testserver"
)

// migratedDB makes a database with the migrations and returns its client. It
// starts no service.
func migratedDB(t *testing.T) *clientpg.Client {
	t.Helper()
	db := env.NewDB(t)
	c, err := clientpg.New(context.Background(), clientpg.Config{URL: db.URL, Database: db.Name, MaxConns: 4, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if err := services.RunMigrations(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return c
}

// I8: the lag gauge has a value for each stream that has the group audit, and
// has no value for a stream without the group. A missing group does not stop the
// refresh for the other streams.
func TestI8_LagWithMissingGroup(t *testing.T) {
	env.ResetAuditStreams(t)
	helpdesk, crm := env.NewProducer(t, "helpdesk"), env.NewProducer(t, "crm")
	helpdesk.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})
	crm.Write(t, audit.Entry{Action: "crm:contact:write", Result: audit.Success})
	admin := env.ValkeyAdmin(t)
	if err := admin.Do(context.Background(), admin.B().Arbitrary("XGROUP", "CREATE", helpdesk.StreamKey(), "audit", "0").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)

	user, pw := env.NewValkeyACLUser(t)
	c := env.ValkeyClient(t, user, pw)
	mp, reader := meterOf(t)
	logger, logs := testserver.NewLogCapture()
	record := executor.FuncWithReturn[commands.RecordEntry, commands.Outcome](
		func(context.Context, commands.RecordEntry) (commands.Outcome, error) {
			return commands.OutcomeStored, nil
		})
	sup, err := intake.New(intake.Config{
		Streams: noConsumers{vkadapter.NewStreams(c, 100, 0, logger, mp)},
		Record:  record, Refresh: time.Minute, Meter: mp.Meter("audit"), Log: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	sup.Refresh(context.Background())

	got := gaugeByStream(t, reader)
	if v, ok := got["helpdesk"]; !ok || v <= 0 {
		t.Fatalf("gauge = %v, want a value more than 0 for helpdesk", got)
	}
	if _, ok := got["crm"]; ok {
		t.Fatalf("gauge = %v, want no value for crm", got)
	}
	if !strings.Contains(logs.Text(), "no consumer group") || !strings.Contains(logs.Text(), "crm") {
		t.Fatalf("no warn record for the missing group: %s", logs.Text())
	}
}

// noConsumers is the real streams adapter with consumers that do nothing, so
// that no consumer makes the missing group again.
type noConsumers struct{ streams.Streams }

type idleRunner struct{}

func (idleRunner) Run(ctx context.Context) error { <-ctx.Done(); return nil }

func (noConsumers) NewConsumer(string, platformstreams.Handler) (streams.Runner, error) {
	return idleRunner{}, nil
}

// I8b: with the intake that cannot store (PostgreSQL stopped), the lag grows.
// After the return of PostgreSQL, the lag goes to 0.
func TestI8_LagGrowsAndFalls(t *testing.T) {
	db, proxy := env.NewProxiedDB(t)
	mp, reader := meterOf(t)
	srv := started(t, testserver.WithDB(db), testserver.WithMeterProvider(mp))
	p := env.NewProducer(t, "helpdesk")
	proxy.Stop()
	p.Write(t, audit.Entry{Action: "helpdesk:ticket:write", Result: audit.Success})

	deadline := time.Now().Add(30 * time.Second)
	for gaugeByStream(t, reader)["helpdesk"] < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the lag is %v after 30 seconds, want 1 s or more", gaugeByStream(t, reader))
		}
		time.Sleep(200 * time.Millisecond)
	}
	proxy.Start()
	waitCount(t, pool(t, srv.DB), 1, selectRows)
	deadline = time.Now().Add(30 * time.Second)
	for {
		if v, ok := gaugeByStream(t, reader)["helpdesk"]; ok && v == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the lag is %v after 30 seconds, want 0", gaugeByStream(t, reader))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// I9: an UPDATE and a TRUNCATE of audit_entries give an error. A DELETE of one
// row works.
func TestI9_AppendOnly(t *testing.T) {
	c := migratedDB(t)
	ctx := context.Background()
	for _, sql := range []string{
		`INSERT INTO audit_entries (entry_id, service, time, action, result, actor, actor_type)
			VALUES (gen_random_uuid(), 'helpdesk', now(), 'helpdesk:ticket:write', 'failure', 'usr_1', 'user')`,
		`INSERT INTO rejected_entries (stream, stream_id, reason, payload) VALUES ('helpdesk', '1-0', 'invalid_json', '\x7b')`,
	} {
		if _, err := c.Pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		`UPDATE audit_entries SET result = 'success'`,
		`TRUNCATE audit_entries`,
		`UPDATE rejected_entries SET reason = 'x'`,
		`TRUNCATE rejected_entries`,
	} {
		_, err := c.Pool.Exec(ctx, sql)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: error = %v, want an append-only error", sql, err)
		}
	}
	if n := count(t, c.Pool, selectRows); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	if tag, err := c.Pool.Exec(ctx, `DELETE FROM audit_entries`); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("delete: %v, rows = %d", err, tag.RowsAffected())
	}
	if tag, err := c.Pool.Exec(ctx, `DELETE FROM rejected_entries`); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("delete: %v, rows = %d", err, tag.RowsAffected())
	}
}

// I17: the intake command gets the same bad message two times. The second call
// gives no error and no second row.
func TestI17_SecondDeliveryOfRejected(t *testing.T) {
	c := migratedDB(t)
	cmd := commands.NewRecordEntry(postgres.New(c.Pool), nil)
	bad := commands.RecordEntry{Stream: "helpdesk", StreamID: "1-0", Payload: []byte(`{"specversion":"1.0"}`)}
	for i := range 2 {
		out, err := cmd.Execute(context.Background(), bad)
		if err != nil || out != commands.OutcomeRejected {
			t.Fatalf("call %d: outcome = %q, error = %v", i+1, out, err)
		}
	}
	if n := count(t, c.Pool, selectRejected); n != 1 {
		t.Fatalf("rejected_entries has %d rows, want 1", n)
	}
	// A stored entry that comes again is a duplicate with no error.
	good := commands.RecordEntry{Stream: "helpdesk", StreamID: "2-0", Payload: rawEvent(t, "helpdesk", nil)}
	for i, want := range []commands.Outcome{commands.OutcomeStored, commands.OutcomeDuplicate} {
		if out, err := cmd.Execute(context.Background(), good); err != nil || out != want {
			t.Fatalf("call %d: outcome = %q, error = %v, want %q", i+1, out, err, want)
		}
	}
}
