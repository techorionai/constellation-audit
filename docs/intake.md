# Intake

The intake reads the audit stream `audit.stream:<app>` of each producer with the consumer group `audit`. It stores each entry one time. The code is in `internal/handlers/worker/intake/` (the supervisor) and in `internal/app/commands/record_entry.go` (the checks and the store).

## Find the producers

The supervisor reads the set `audit.apps` with `streamsvalkey.AuditApps` at the start and at each `AUDIT_INTAKE__REFRESH`. Only the provisioner (the Valkey admin) writes the set. It registers each producer when it makes the ACL user of the producer (`SADD audit.apps <app>`). A producer cannot write the set, so a forged name is not possible. `AuditApps` counts a member only when its stream exists. It deletes no member. A registered app with no stream is examined again at each refresh. A producer that is not registered is not found.

`AuditApps` reads a maximum of 10000 members of the set.

- The supervisor starts one consumer for each new app. The group is `audit` and the start ID is `0`, so the intake also reads the entries that a producer wrote before the audit service saw its app.
- The supervisor keeps each app that has a consumer in the table `known_apps` of PostgreSQL. At each refresh, it reads the table and starts the consumers of the known apps before it reads the set. The table keeps the consumers of the known apps in three cases: `audit.apps` is empty, an app is not in the set, or the reader cannot read the set (`apps_error`). If the supervisor cannot read the table, it counts the event (reason `known_apps_error`) and goes on with the set.
- The supervisor never stops a consumer when an app leaves the set.
- If a consumer stops with an error while the service runs, the supervisor counts the event (reason `consumer_stopped`), logs it, and starts a new consumer for that app at the next refresh.
- The limit is 1000 apps. `AuditApps` returns the first 1000 and `ErrTooManyAuditApps` when more than 1000 registered apps have a stream, or when the set has more than 10000 members. Both cases are an operator error, not an attack. The supervisor logs one warn record that names the limit, counts the event (`constellation.audit.intake.problems`, reason `too_many_apps`), and uses the list. The supervisor also keeps its own limit of 1000 consumers. The producers above the limit are not read.
- On another error, the supervisor logs and counts the error, keeps the current consumers, and tries again at the next refresh.

## Checks

The consumer uses `NeverDrop`. If the store fails, the handler returns an error, so the entry stays pending and comes again. The handler checks each message before it stores it:

1. A message of more than 64 KiB gives `too_large`. JSON that does not decode gives `invalid_json`. A member name that occurs two times, and a name of a known member of the envelope, of `data`, or of `target` with other letter case (for example `Source`), give `invalid_json`. The decoder ignores unknown members of the envelope and of `data`.
2. Envelope: `specversion` is `1.0`, `source` is `/<app>`, `type` is `<app>.audit.recorded`, `id` is a UUID in lowercase text form, `time` is present and is not more than 5 minutes after the time of receipt (a later time gives `invalid_time`), `actor` has 1 to 128 characters and `actortype` is a principal type, `act` and `acttype` go together, and a `traceparent` matches the W3C pattern.
3. Data: `action` has the permission key pattern and starts with `<app>:`, `result` is one of 4 values, and the target, summary, details (16 KiB), request ID, IP address (an IPv4 or IPv6 address of 1 to 45 characters, with no port and no zone), and user agent are in their limits. No text field has the character U+0000, and the JSON of `details` has no JSON escape of U+0000.

The service of a stored entry is always the app of the stream key. The app of the entry does not count.

### Reason codes

`too_large`, `invalid_json`, `invalid_specversion`, `invalid_source`, `invalid_type`, `invalid_id`, `invalid_time`, `invalid_actor`, `invalid_act`, `invalid_traceparent`, `invalid_action`, `invalid_result`, `invalid_target`, `invalid_summary`, `invalid_details`, `invalid_request_id`, `invalid_ip_address`, `invalid_user_agent`, `invalid_text`, and `store_refused`.

`store_refused` means that PostgreSQL refused the data with an error of class 22 (data exception) and the checks did not find the reason. A retry cannot fix such an entry. Errors of the classes 08 and 53 mean that the store is not available. The entry stays pending.

## Delivery after an error

A pending message comes again after the claim idle time of platform-go, 5 minutes by default. After a stop of PostgreSQL, the entries of that time are thus stored up to 5 minutes after the return of the database. No entry is lost, because the group keeps the message until the handler acknowledges it.

## Rejected entries

An entry that fails a check goes to the table `rejected_entries`. The row has the stream, the stream ID, the event ID (if any), the reason code, and the raw payload. The insert uses `ON CONFLICT (stream, stream_id) DO NOTHING`, and the handler then acknowledges the message. A second delivery of the same message gives no error. The retention job deletes old rows (see `retention.md`).

The log record of a rejected entry has the stream, the stream ID, and the reason code only.

### Messages that the consumer drops

The consumer of platform-go acknowledges and drops some messages before the handler. These messages are not in `rejected_entries`:

- a message that is larger than 256 KiB (`MaxMessageBytes`),
- a message whose `source` is not `/<stream app>`,
- a message whose `type` does not start with `<stream app>.`.

The consumer logs each drop at error level and counts it in `constellation.events.consumer.handled` with the attribute `outcome` set to `invalid`. `NeverDrop` does not stop these drops. Alert on this counter.

## Append-only rule

A trigger refuses each `UPDATE` and each `TRUNCATE` on `audit_entries` and `rejected_entries`. `DELETE` stays possible for the retention job. The trigger stops a code error that changes a stored entry. It does not stop an owner of the database. A hash chain that detects such a change is not in this version.

## Lag

`constellation.audit.intake.lag` is the age in seconds of the oldest entry that the group `audit` did not acknowledge, for each stream. The supervisor reads it at each refresh. Each read has a time limit of 5 seconds. If a read fails, the gauge has no value for that stream.

- If the group does not exist, the gauge has no value for that stream. The supervisor logs a warn record with the stream, and the refresh continues for the other streams.
- The producers trim their audit streams after 7 days. If the intake stops for longer than that, entries are lost. Alert on a lag that grows.

## Other counters

`constellation.audit.intake.entries` counts the handled messages with the attribute `outcome`: `stored`, `duplicate`, `rejected`, or `error`.
