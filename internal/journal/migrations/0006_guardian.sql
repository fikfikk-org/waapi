-- 0006_guardian.sql: sinyal kesehatan akun per instance
CREATE TABLE contacts_seen (
    instance_id  TEXT    NOT NULL,
    jid          TEXT    NOT NULL,          -- PN kanonik bila diketahui, selain itu LID
    first_in     INTEGER,                   -- pesan masuk pertama dari kontak ini (unix detik)
    last_in      INTEGER,
    first_out    INTEGER,
    last_out     INTEGER,
    opted_out    INTEGER NOT NULL DEFAULT 0,
    opt_changed  INTEGER,
    PRIMARY KEY (instance_id, jid)
) WITHOUT ROWID;

CREATE TABLE guardian_hourly (
    instance_id        TEXT    NOT NULL,
    hour               INTEGER NOT NULL,    -- unix detik / 3600
    sent               INTEGER NOT NULL DEFAULT 0,
    first_contact_sent INTEGER NOT NULL DEFAULT 0,
    delivered          INTEGER NOT NULL DEFAULT 0,
    replied            INTEGER NOT NULL DEFAULT 0,
    groups_created     INTEGER NOT NULL DEFAULT 0,
    rate_overlimit     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, hour)
) WITHOUT ROWID;

CREATE TABLE guardian_fanout (
    instance_id  TEXT NOT NULL,
    day          TEXT NOT NULL,             -- YYYY-MM-DD di zona waktu instance
    content_hash BLOB NOT NULL,             -- sha256(teks ternormalisasi)[:16]
    rcpt         TEXT NOT NULL,
    PRIMARY KEY (instance_id, day, content_hash, rcpt)
) WITHOUT ROWID;

CREATE TABLE health_log (
    id           INTEGER PRIMARY KEY,
    instance_id  TEXT    NOT NULL,
    at           INTEGER NOT NULL,
    kind         TEXT    NOT NULL,          -- temp_ban, state_change, rate_overlimit
    code         INTEGER NOT NULL DEFAULT 0,
    detail       TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX ix_health_log_inst_at ON health_log(instance_id, at);

CREATE TABLE guardian_shadow_meta (
    instance_id TEXT    PRIMARY KEY,
    booted_at   INTEGER NOT NULL
) WITHOUT ROWID;
