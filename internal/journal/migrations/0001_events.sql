-- 0001_events.sql: journal event masuk. seq AUTOINCREMENT agar tidak pernah dipakai ulang setelah pruning
CREATE TABLE events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  TEXT    NOT NULL,
    type         TEXT    NOT NULL,
    chat         TEXT    NOT NULL DEFAULT '',
    sender       TEXT    NOT NULL DEFAULT '',
    wa_id        TEXT,
    dedupe_key   TEXT,
    ts           INTEGER NOT NULL,          -- unix detik menurut WhatsApp
    received_at  INTEGER NOT NULL,          -- unix milidetik saat diterima
    part         INTEGER NOT NULL,          -- fnv32a(chat) % 64
    flags        INTEGER NOT NULL DEFAULT 0, -- bit0 offline, bit1 truncated
    payload      BLOB    NOT NULL           -- JSON data, maksimal 64 KiB
);
CREATE UNIQUE INDEX ux_events_dedupe ON events(instance_id, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX ix_events_inst_seq ON events(instance_id, seq);
CREATE INDEX ix_events_inst_waid ON events(instance_id, chat, wa_id) WHERE wa_id IS NOT NULL;
CREATE INDEX ix_events_received ON events(received_at);

-- Posisi baca tiap konsumen (webhook per lane, hooks, dst.)
CREATE TABLE cursors (
    consumer     TEXT    NOT NULL,
    instance_id  TEXT    NOT NULL,
    lane         INTEGER NOT NULL DEFAULT 0,
    seq          INTEGER NOT NULL DEFAULT 0,
    enabled      INTEGER NOT NULL DEFAULT 1,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (consumer, instance_id, lane)
) WITHOUT ROWID;
