-- The retention job deletes audit_entries by the time of receipt.
CREATE INDEX audit_entries_received_at_idx ON audit_entries (received_at);

-- The apps whose audit streams the intake found. At startup, the intake starts
-- a consumer for each of them before it reads the set audit.apps in Valkey. The
-- consumers of these apps stay if the set is empty or the reader cannot read it.
CREATE TABLE known_apps (
    app           text        PRIMARY KEY,
    first_seen_at timestamptz NOT NULL DEFAULT now()
);
