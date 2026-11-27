# Architecture

The audit service keeps the append-only audit trail of the platform. Each service writes its audit entries in the same database transaction as the change, through the audit outbox of `constellation-platform-go`. The outbox relay adds each entry to the Valkey stream `audit.stream:<app>`. The audit service reads these streams and stores each entry one time. See section 7 of `VISION.md` in the planner repository.

This version has the intake and the retention. The three operations of the API (search, get, export) give 501 `not_implemented`. The next version replaces them.

## Layout

The code follows the hexagonal architecture guide of the planner repository.

| Folder | Content |
| ------ | ------- |
| `cmd/audit/` | `main.go` (`serve`) and `healthcheck.go`. |
| `api/openapi/` | Copy of the OpenAPI file and of the permission manifest from `constellation-contracts`. `CONTRACTS_VERSION` gives the release tag. |
| `internal/domain/` | The audit entry, the checks of the contract, and the reason codes. It imports the standard library only. |
| `internal/ports/entries/` | The port of the store of entries and rejected entries. |
| `internal/ports/streams/` | The port of the audit streams: the list of producers, the lag, and the consumers. |
| `internal/adapters/postgres/` | The PostgreSQL adapter of the entries port. |
| `internal/adapters/valkey/` | The Valkey adapter of the streams port. It uses `streams/valkey` of platform-go. |
| `internal/app/commands/` | The use case `RecordEntry`. |
| `internal/handlers/worker/intake/` | The intake supervisor: the list of producers, one consumer for each producer, and the lag gauge. |
| `internal/handlers/http/` | The HTTP handlers and the scope table. |
| `internal/handlers/http/gen/` | The generated server code. |
| `internal/services/` | The composition root: startup, shutdown, readiness. |
| `internal/config/` | The configuration. |
| `migrations/` | The SQL migrations. |
| `resources/test/` | The test helpers (including the mocks of the ports) and the lint and image tests. |

The depguard rules of `.golangci.yml` check the layers. The lint test in `resources/test/lint` tests the rules.

## Routes

| Route | Operation | Scope | State |
| ----- | --------- | ----- | ----- |
| `GET /v1/audit-entries` | `AuditEntries_list` | `audit:log:read` | 501 `not_implemented` |
| `GET /v1/audit-entries/export` | `AuditEntries_export` | `audit:log:read` | 501 `not_implemented` |
| `GET /v1/audit-entries/{id}` | `AuditEntries_get` | `audit:log:read` | 501 `not_implemented` |

The scope table reads the `x-required-scopes` value of each operation from the embedded OpenAPI file. A `/v1` route with no entry gives 500 (fail closed). The token check and the scope check run before the handler, so a request with no valid token gives 401, and a request without the scope gives 403.

The service accepts JWT access tokens with the single audience `audit`. It does not accept personal API tokens.

## Startup order

1. Load the config. Set up telemetry with the service name `audit`.
2. Connect PostgreSQL and Valkey. Start the admin server. `/readyz` is 503.
3. Run the migrations with the advisory lock.
4. Start the publisher of the own permission manifest. The checker of the publisher is part of the readiness.
5. Start the intake supervisor and the retention runner. They start after the migrations, because they use the tables.
6. Start the public server. Mark the service as started. `/readyz` is 200 when all checks pass.

Shutdown: drain the admin server, stop the public server, stop the intake (each consumer leaves its group with a limit of 5 s, and the supervisor waits for all consumers), stop the retention runner and the publisher, close the clients, stop the admin server.

The intake has no effect on `/readyz`. A Valkey error of the intake is logged and counted, and the supervisor tries again at the next refresh. See `intake.md`.

Identity gives a token for the audience `audit` only after it has the manifest of the audit service.

## Data model

| Table | Content |
| ----- | ------- |
| `audit_entries` | One row for each audit entry. The unique key is `(service, entry_id)`. `service` is the app of the stream. |
| `rejected_entries` | One row for each stream message that failed a check. The unique key is `(stream, stream_id)`. It keeps the raw payload. |

A trigger refuses each `UPDATE` and each `TRUNCATE` on both tables. `DELETE` stays possible for the retention job. See `intake.md` and `retention.md`.

## Telemetry

- Spans for each request and each command. The consumer span `events.consume` comes from platform-go.
- `constellation.audit.intake.entries` (counter): the stream messages that the intake handled. The attribute `outcome` is `stored`, `duplicate`, `rejected`, or `error`.
- `constellation.audit.intake.lag` (gauge, `s`): the age of the oldest entry that the group `audit` did not acknowledge. The attribute `stream` is the app.
- `constellation.audit.intake.problems` (counter): problems of the supervisor. The attribute `reason` is `too_many_apps`, `apps_error`, `consumer_error`, or `consumer_stopped`.
- `constellation.events.consumer.handled` (counter, platform-go): also shows the messages that the consumer drops before the handler.
- `retention.*` (platform-go): the retention jobs `audit_entries` and `rejected_entries`.

No log record and no span has the summary, the details, the IP address, the user agent, or the payload of an entry.
