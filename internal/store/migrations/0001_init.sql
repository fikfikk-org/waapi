-- Instance WhatsApp yang dikelola daemon
CREATE TABLE IF NOT EXISTS instances (
    id              TEXT PRIMARY KEY,
    jid             TEXT UNIQUE,
    status          TEXT NOT NULL DEFAULT 'created'
                    CHECK (status IN ('created','pairing','connected','disconnected','logged_out','disabled')),
    webhook_url     TEXT,
    webhook_secret  TEXT NOT NULL,
    webhook_events  TEXT NOT NULL DEFAULT 'message,receipt,connection',
    rate_per_min    INTEGER NOT NULL DEFAULT 20,
    auto_start      INTEGER NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

-- API key; hanya hash SHA-256 yang disimpan
CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    prefix       TEXT NOT NULL,
    hash         BLOB NOT NULL UNIQUE,
    instance_id  TEXT REFERENCES instances(id) ON DELETE CASCADE,
    created_at   INTEGER NOT NULL,
    last_used_at INTEGER,
    revoked_at   INTEGER
);

-- Outbox webhook untuk event gagal kirim atau saat antrian memori penuh
CREATE TABLE IF NOT EXISTS webhook_outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   TEXT NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
    event         TEXT NOT NULL,
    payload       BLOB NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER NOT NULL,
    created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON webhook_outbox(next_retry_at);

