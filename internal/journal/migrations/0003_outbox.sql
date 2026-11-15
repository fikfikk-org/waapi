-- 0003_outbox.sql: antrian pesan keluar yang tahan restart; semua waktu dalam unix milidetik
CREATE TABLE outbox (
    id              TEXT    PRIMARY KEY,
    instance_id     TEXT    NOT NULL,
    idem_key        TEXT,
    req_hash        BLOB,
    wa_id           TEXT    NOT NULL,
    to_jid          TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    spec            BLOB    NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,       -- 0 normal, 10 urgent
    origin          TEXT    NOT NULL DEFAULT 'api',    -- api, v1, cli, hook, mcp, ai, system
    key_id          TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL CHECK (status IN ('queued','sending','sent','delivered','read','played','failed','cancelled','expired')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    not_before      INTEGER NOT NULL,
    lease_until     INTEGER,
    expires_at      INTEGER NOT NULL,
    error_code      TEXT    NOT NULL DEFAULT '',
    error_msg       TEXT    NOT NULL DEFAULT '',
    sent_at         INTEGER,
    delivered_at    INTEGER,
    read_at         INTEGER,
    rcpt_delivered  INTEGER NOT NULL DEFAULT 0,
    rcpt_read       INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE UNIQUE INDEX ux_outbox_idem ON outbox(instance_id, idem_key) WHERE idem_key IS NOT NULL;
CREATE UNIQUE INDEX ux_outbox_waid ON outbox(instance_id, wa_id);
CREATE INDEX ix_outbox_due ON outbox(instance_id, status, priority DESC, not_before, id);
CREATE INDEX ix_outbox_terminal ON outbox(status, updated_at);
