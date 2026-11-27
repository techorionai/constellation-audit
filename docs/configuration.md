# Configuration

The service reads its settings from environment variables with the prefix `AUDIT_`. A double underscore `__` separates the levels of a nested name. A variable with a `_FILE` form reads its value from a file where the platform-go package says so.

## Variables

| Variable | Default | Rule | Use |
| -------- | ------- | ---- | --- |
| `AUDIT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` | Log level. |
| `AUDIT_HTTP__ADDR` | `:8080` | | Public listener. |
| `AUDIT_ADMIN__ADDR` | `:9090` | | Listener for `/livez` and `/readyz`. Do not publish it at the edge. |
| `AUDIT_ADMIN__CHECK_TIMEOUT` | `2s` | | Time limit of the readiness checks. |
| `AUDIT_ADMIN__DRAIN_DELAY` | `5s` | | Time that `/readyz` is 503 before the public server stops. |
| `AUDIT_DATABASE__URL`, `AUDIT_DATABASE__NAME` | required | platform-go `clients/postgres` rules | PostgreSQL. Also `AUDIT_DATABASE__URL_FILE`. |
| `AUDIT_DATABASE__MAX_CONNS` | `10` | `4 + 2 x AUDIT_EXPORT__CONCURRENCY` or more | Size of the connection pool. The retention runner needs 2 connections for each tick. Each export holds the connection of its read and takes a second connection for its own audit entry. With the default concurrency 2, the smallest value is 8. A smaller value stops the service at start with a message that names this variable. |
| `AUDIT_VALKEY__ADDRESS`, `AUDIT_VALKEY__KEY_PREFIX` | required | platform-go `clients/valkey` rules | Valkey. Use the key prefix `audit`. |
| `AUDIT_AUTH__ISSUER` | required | `https`, or `http` with `AUDIT_AUTH__ALLOW_INSECURE_HTTP` | Issuer of the identity service. It is the `iss` claim that the verifier requires. |
| `AUDIT_AUTH__JWKS_URL` | `<issuer>/.well-known/jwks.json` | | URL of the key set of identity. |
| `AUDIT_AUTH__REFRESH_INTERVAL`, `AUDIT_AUTH__MIN_REFETCH_INTERVAL`, `AUDIT_AUTH__LEEWAY`, `AUDIT_AUTH__HTTP_TIMEOUT` | `10m`, `30s`, `30s`, `5s` | Same as `auth/jwt.Config` of platform-go | Verifier settings. |
| `AUDIT_AUTH__ALLOW_INSECURE_HTTP` | `false` | | Allows `http` for the issuer. Use it only for local compose and tests. |
| `AUDIT_CLIENT__*` | required | `auth/clientcred.Config` of platform-go | The service client `audit` of identity: `ID`, `KEY_ID`, `PRIVATE_KEY` (or `PRIVATE_KEY_FILE`), `ISSUER`, `TOKEN_URL`. The service uses it to publish its manifest. |
| `AUDIT_IDENTITY__URL` | required | `clients/identity.Config` of platform-go | Internal URL of identity. Also `AUDIT_IDENTITY__ALLOW_INSECURE_HTTP`. |
| `AUDIT_INTAKE__REFRESH` | `30s` | `5s` to `5m` | Time between two reads of the list of producers. A new producer gets its consumer at the next refresh. |
| `AUDIT_INTAKE__COUNT` | `100` | 1 to 1000 | Maximum number of messages of one read of a stream. |
| `AUDIT_RETENTION__AUDIT_ENTRIES__PERIOD` | `1440h` (60 days) | | Age after which the retention job deletes an entry. See `retention.md`. |
| `AUDIT_RETENTION__REJECTED_ENTRIES__PERIOD` | `1440h` (60 days) | | Age after which the retention job deletes a rejected entry. |
| `AUDIT_RETENTION__<JOB>__INTERVAL`, `AUDIT_RETENTION__<JOB>__BATCH_SIZE` | `1h`, `1000` | `retention.Config` of platform-go | Time between two ticks, and rows for each delete. `<JOB>` is `AUDIT_ENTRIES` or `REJECTED_ENTRIES`. |
| `AUDIT_HTTP__TRUSTED_PROXIES` | empty | Comma list of CIDR prefixes, for example `10.0.0.0/8,192.168.0.0/16` | Addresses of your own reverse proxies. The service reads `X-Forwarded-For` only from a connection from these addresses. The audit entry of an export records the client IP address that results. Set only the addresses of proxies that you control. A wrong value records a wrong IP address. |
| `AUDIT_EXPORT__MAX_ENTRIES` | `100000` | 1 to 1000000 | Largest number of entries of one export. More entries give 422 `export_too_large`. |
| `AUDIT_EXPORT__CONCURRENCY` | `2` | 1 to 16 | Number of exports that one instance runs at the same time. More give 429 `too_many_exports`. The default is the value of the contract. An operator with a large database can set a higher value. |
| `AUDIT_EXPORT__TIMEOUT` | `10m` | `1m` to `1h` | Time limit to write the body of one export. |

The audience of the service is the constant `audit`. The service does not read `AUDIT_AUTH__AUDIENCE`. A value in that variable has no effect.

The service stops at start with a message that names the variable when a rule fails.

## Valkey ACL

The Valkey user of the audit service needs the key pattern `~audit:*` and the command rule of platform-go. It also needs the two selectors of `streamsvalkey.AuditReaderSelectors()`:

- the audit streams `audit.stream:*`: `+xreadgroup +xack +xautoclaim +xpending +xinfo|groups +xinfo|stream +xinfo|consumers +xgroup|create +xgroup|createconsumer +xgroup|delconsumer +xrange`,
- the set `audit.apps`: `(~audit.apps +sscan)`.

The reader has no write command on the set. The provisioner registers each producer in `audit.apps` as admin. Without `+sscan` on `audit.apps`, each refresh counts `apps_error`, `known_apps` keeps the known apps, and a new producer is not found. To check the ACL, run `ACL DRYRUN <reader user> SSCAN audit.apps 0`. It must return `OK`. Run `ACL DRYRUN <reader user> SREM audit.apps x`. It must give an error.

Upgrade from `v0.1.x`: follow the order of the section "Upgrade from v0.8.0 to v0.9.0" of constellation-platform-go. Before the `v0.2.0` image starts, register the producers and give the reader the transition selectors (the selectors of `v0.9.0` and `(~audit.streams +sscan)`). After the update, the reader needs only the selectors of `v0.9.0`. The update of `constellation-deploy` does these steps.

Give these selectors only to the audit service user. A producer user gets the selectors of `streamsvalkey.AuditProducerSelectors(app)`. They give no read access.

## Commands

| Command | Use |
| ------- | --- |
| `audit` | Start the server. |
| `audit healthcheck` | Exit 0 if `/readyz` of the local admin port is 200. Else exit 1. The container health check uses it. It reads `AUDIT_ADMIN__ADDR`. |

Telemetry uses the standard `OTEL_*` variables. See the `telemetry` package of `constellation-platform-go`.

## The service client

The service needs a client in the clients file of identity. The client has the ID `audit` and the app `audit`. It needs no role, because the implicit role `identity:service` gives it the right to publish its manifest.
