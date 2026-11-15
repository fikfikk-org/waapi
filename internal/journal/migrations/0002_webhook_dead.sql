-- 0002_webhook_dead.sql: event yang gagal dikirim setelah batas percobaan
CREATE TABLE webhook_dead (
    instance_id  TEXT    NOT NULL,
    seq          INTEGER NOT NULL,
    attempts     INTEGER NOT NULL,
    last_status  INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT    NOT NULL DEFAULT '',
    dead_at      INTEGER NOT NULL,
    PRIMARY KEY (instance_id, seq)
) WITHOUT ROWID;
