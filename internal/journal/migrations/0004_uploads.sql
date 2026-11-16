-- 0004_uploads.sql: descriptor media yang sudah diunggah ke CDN WhatsApp, bisa dipakai ulang sampai kedaluwarsa.
CREATE TABLE IF NOT EXISTS uploads (
    id          TEXT    PRIMARY KEY,
    instance_id TEXT    NOT NULL,
    kind        TEXT    NOT NULL,
    mimetype    TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    descriptor  BLOB    NOT NULL,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_uploads_expires ON uploads(expires_at);
