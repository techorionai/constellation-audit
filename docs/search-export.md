# Search, get, and export

The audit service has 3 read operations. All need the scope `audit:log:read`. All answers, also the errors, have `Cache-Control: no-store` and `Pragma: no-cache`.

| Route | Operation |
| ----- | --------- |
| `GET /v1/audit-entries` | `AuditEntries_list`. Search with filters and pages. |
| `GET /v1/audit-entries/{id}` | `AuditEntries_get`. One entry. |
| `GET /v1/audit-entries/export` | `AuditEntries_export`. All matching entries as CSV or JSON. |

## Filters

The list and the export have the same filters. All are optional.

| Parameter | Rule |
| --------- | ---- |
| `from` | Time in RFC 3339 form. The range includes it. |
| `to` | Time in RFC 3339 form. The range excludes it. `from` must be before `to`. |
| `service` | App key: `^[a-z][a-z0-9_]*$`, a maximum of 32 characters. |
| `action` | Permission key, a maximum of 128 characters. |
| `result` | `success`, `denied`, `invalid`, or `failure`. |
| `actor`, `act` | 1 to 128 characters. |
| `actorType` | `user`, `agent`, `service`, or `workflow`. |
| `targetType` | `^[a-z][a-z0-9_]{0,63}$`. |
| `targetId` | 1 to 256 characters. It needs `targetType`. |
| `requestId` | 1 to 128 characters. |
| `traceId` | 32 lowercase hex characters. |

Each text value must be valid UTF-8 and must not have the character U+0000. A value that breaks a rule gives 400 `invalid_parameter`. The detail names the parameter. It never has the value. A PostgreSQL data error on a read query also gives 400 `invalid_parameter`. A database that is not available gives 503 `unavailable` with `Retry-After: 5`.

## Order and pages

- The order is `time` descending, then the internal key descending. The internal key is the `id` column of the table. It is not the `id` of the API.
- `pageSize` is 1 to 200. The default is 50. Another value gives 400 `invalid_parameter`.
- A page also ends at 1 MiB of response body. A page has one item at least.
- `nextPageToken` is a base64url JSON value with the operation, the SHA-256 hash of the filters, and the cursor `(time, internal key)` of the last item. The next page starts after this cursor, so new entries do not change the pages that follow.
- A token for other filters, a token of another operation, a token that does not decode, and a token of more than 1024 characters give 400 `invalid_page_token`.

## Get

- `id` is a UUID. A value that is not a UUID gives 404 `audit_entry_not_found`. A UUID in upper case finds the entry. The `id` of the answer is always lowercase.
- The unique key of the table is `(service, entry_id)`. Two services can have the same `id`. The optional parameter `service` selects the entry of one service.
- Without `service`, the answer is the entry with the oldest `receivedAt`, then the smallest internal key.
- A `service` that breaks the app key rule gives 400 `invalid_parameter`. An unknown `service` or `id` gives 404.

## Export

`format` is required: `csv` or `json`. Another value gives 400 `invalid_parameter`.

- The export uses the filters and the order of the list. It has no pages.
- The service counts the matching entries first. More than `AUDIT_EXPORT__MAX_ENTRIES` entries (default 100000) give 422 `export_too_large`, before the first byte of data.
- The service reads all entries in one `REPEATABLE READ READ ONLY` transaction with a server-side cursor, in batches of 1000. The count and the body come from the same snapshot. The service writes the body as it reads, so the memory use is small.
- The headers are `Content-Disposition: attachment; filename="audit-entries-<UTC time>.<csv or json>"`, for example `audit-entries-20261127T101500Z.csv`, and `X-Content-Type-Options: nosniff`.
- The write deadline of the response is `AUDIT_EXPORT__TIMEOUT` (default 10 minutes), because a large export takes longer than the normal write timeout.

### Limits

An instance runs a maximum of `AUDIT_EXPORT__CONCURRENCY` exports at the same time. The default is 2, the value of the contract. An operator can change it from 1 to 16. A pool that has too few connections stops the service at start. See `configuration.md`. An export above the limit gives 429 `too_many_exports` with `Retry-After: 10`.

The service does not set a limit for each principal.

### CSV

- The file follows RFC 4180: the line end is `\r\n`, the first row is the header row, each cell is in double quotes, and a `"` in a cell is `""`.
- The columns, in order: `id`, `service`, `time`, `receivedAt`, `action`, `result`, `actor`, `actorType`, `act`, `actType`, `targetType`, `targetId`, `summary`, `requestId`, `traceId`, `ipAddress`, `userAgent`, `details`. Times are in RFC 3339 form in UTC. `details` is JSON text. An optional value that is not set is an empty cell.
- A cell that starts with `=`, `+`, `-`, `@`, a tab, a carriage return, or a line feed gets a `'` in front. This rule applies to all columns. A spreadsheet then does not run the cell as a formula.

### JSON

The body is one JSON array of `AuditEntry` objects. The service writes `[`, then each entry with a `,` between entries, then `]`. An export of no entries is `[]`.

### An error after the first byte

The service reads the first batch before it writes the headers, so a database error at the start gives a normal Problem answer. After the first byte, the service cannot change the status. It logs the error and aborts the connection. The client sees an incomplete body and not a short file that looks complete. The client must not use an export that ends with an error.

## The audit entry of an export

An export copies many entries out of the service. The service writes an audit entry for each export request, directly into `audit_entries`, after the count and before the first byte of data. A list and a get write no entry.

| Column | Value |
| ------ | ----- |
| `entry_id` | A new UUIDv7 in lowercase text form. |
| `service` | `audit` |
| `time` | The time of the decision. |
| `action` | `audit:log:export` |
| `result` | `success`. `invalid` for a 422. `denied` for a 429. |
| `actor`, `actor_type` | The subject and the type of the verified principal. |
| `act`, `act_type` | The `act` principal of the token, if set. |
| `summary` | `Export of audit entries` |
| `details` | `format`, `filters` (only the set filters), and `count`. A 422 has `maxEntries` in place of `count`. |
| `request_id` | The `X-Request-ID` of the request, if it matches `^[A-Za-z0-9._-]{1,128}$`. |
| `trace_id` | The trace ID of the request, if the request has a valid trace. |
| `ip_address` | The client address from the `clientinfo` middleware, if it is valid and has no zone. |
| `user_agent` | The `User-Agent`, if it has 1 to 512 characters of valid UTF-8 with no U+0000. |

A value that breaks its rule is left out. It does not stop the export. If the service cannot write the entry of a successful export, the answer is 503 and the service sends no data.

The client IP address depends on `AUDIT_HTTP__TRUSTED_PROXIES`. See `configuration.md`.

## Telemetry

- The counter `constellation.audit.export` has the attributes `format` and `outcome` (`ok`, `too_large`, `busy`, or `error`).
- No log record and no span has a filter value, a summary, details, an IP address, or a user agent. The log record of an export has the principal ID, the format, the count, and the outcome.
