CREATE TABLE audit_entries (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id    uuid        NOT NULL,
    service     text        NOT NULL,
    time        timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    action      text        NOT NULL,
    result      text        NOT NULL,
    actor       text        NOT NULL,
    actor_type  text        NOT NULL,
    act         text        NULL,
    act_type    text        NULL,
    target_type text        NULL,
    target_id   text        NULL,
    summary     text        NULL,
    details     jsonb       NULL,
    request_id  text        NULL,
    trace_id    text        NULL,
    ip_address  inet        NULL,
    user_agent  text        NULL,
    UNIQUE (service, entry_id)
);

CREATE INDEX audit_entries_time_idx ON audit_entries (time DESC, id DESC);
CREATE INDEX audit_entries_service_time_idx ON audit_entries (service, time DESC, id DESC);
CREATE INDEX audit_entries_action_time_idx ON audit_entries (action, time DESC, id DESC);
CREATE INDEX audit_entries_actor_time_idx ON audit_entries (actor, time DESC, id DESC);
CREATE INDEX audit_entries_target_time_idx ON audit_entries (target_type, target_id, time DESC, id DESC);
CREATE INDEX audit_entries_request_id_idx ON audit_entries (request_id);
CREATE INDEX audit_entries_trace_id_idx ON audit_entries (trace_id);
CREATE INDEX audit_entries_entry_id_idx ON audit_entries (entry_id);

CREATE TABLE rejected_entries (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream      text        NOT NULL,
    stream_id   text        NOT NULL,
    entry_id    text        NULL,
    reason      text        NOT NULL,
    payload     bytea       NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream, stream_id)
);

CREATE INDEX rejected_entries_received_at_idx ON rejected_entries (received_at);

-- The trail is append-only. UPDATE and TRUNCATE are refused. DELETE stays
-- possible for the retention job.
CREATE FUNCTION audit_refuse_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is not allowed: the table is append-only', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER audit_entries_no_update BEFORE UPDATE ON audit_entries
    FOR EACH ROW EXECUTE FUNCTION audit_refuse_change();
CREATE TRIGGER audit_entries_no_truncate BEFORE TRUNCATE ON audit_entries
    FOR EACH STATEMENT EXECUTE FUNCTION audit_refuse_change();
CREATE TRIGGER rejected_entries_no_update BEFORE UPDATE ON rejected_entries
    FOR EACH ROW EXECUTE FUNCTION audit_refuse_change();
CREATE TRIGGER rejected_entries_no_truncate BEFORE TRUNCATE ON rejected_entries
    FOR EACH STATEMENT EXECUTE FUNCTION audit_refuse_change();
