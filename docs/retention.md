# Retention

The service has two retention jobs. They use the retention runner of `constellation-platform-go`. The runner takes an advisory lock for each job, so only one instance runs a job at one time. The runner needs 2 connections for each tick, so the pool has 4 connections or more.

| Job | Table | Time column | Default period |
| --- | ----- | ----------- | -------------- |
| `audit_entries` | `audit_entries` | `time` (the time of the action) | 60 days |
| `rejected_entries` | `rejected_entries` | `received_at` | 60 days |

A job deletes the rows that are older than its period, in batches.

## Variables

| Variable | Default | Use |
| -------- | ------- | --- |
| `AUDIT_RETENTION__AUDIT_ENTRIES__PERIOD` | `1440h` | Age of an entry after which the job deletes it. |
| `AUDIT_RETENTION__REJECTED_ENTRIES__PERIOD` | `1440h` | Age of a rejected entry after which the job deletes it. |
| `AUDIT_RETENTION__AUDIT_ENTRIES__INTERVAL`, `AUDIT_RETENTION__REJECTED_ENTRIES__INTERVAL` | `1h` | Time between two ticks. 1 minute or more. |
| `AUDIT_RETENTION__AUDIT_ENTRIES__BATCH_SIZE`, `AUDIT_RETENTION__REJECTED_ENTRIES__BATCH_SIZE` | `1000` | Rows for each delete statement. |

## Compliance note

Section 15 of `VISION.md` gives 60 days as the default period of the audit trail. A longer period can be necessary for a legal rule or a contract. The operator sets the period for the install. A shorter period than the rule is the responsibility of the operator.

The retention job deletes by the time of the action. An entry with a `time` in the past, for example from a producer with a wrong clock, can be deleted at the first tick.

The trigger on the tables refuses `UPDATE` and `TRUNCATE`, but not `DELETE`. The retention job needs `DELETE`.
