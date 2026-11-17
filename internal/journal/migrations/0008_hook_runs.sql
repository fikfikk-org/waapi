-- 0008_hook_runs.sql: jejak eksekusi per (instance, seq, hook) untuk idempotency, suppression tahan restart, dan audit
CREATE TABLE hook_runs (
    instance_id TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    hook_id     TEXT    NOT NULL,
    status      TEXT    NOT NULL,   -- matched, done, error, skipped_stale, skipped_takeover, throttled, skipped_pressure
    detail      TEXT    NOT NULL DEFAULT '',
    suppressed  INTEGER NOT NULL DEFAULT 0,
    at          INTEGER NOT NULL,
    PRIMARY KEY (instance_id, seq, hook_id)
) WITHOUT ROWID;
CREATE INDEX ix_hook_runs_inst_at ON hook_runs(instance_id, at DESC);
