-- 0004_v2_keys_audit.sql: scope, expiry, rate limit, policy per key, dan audit log aksi admin
ALTER TABLE api_keys ADD COLUMN scopes TEXT NOT NULL DEFAULT '*';
ALTER TABLE api_keys ADD COLUMN expires_at INTEGER;
ALTER TABLE api_keys ADD COLUMN rate_per_sec INTEGER;
ALTER TABLE api_keys ADD COLUMN policy TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS audit_log (
    id      INTEGER PRIMARY KEY,
    at      INTEGER NOT NULL,
    actor   TEXT    NOT NULL,
    action  TEXT    NOT NULL,
    target  TEXT    NOT NULL DEFAULT '',
    detail  TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_audit_at ON audit_log(at);
