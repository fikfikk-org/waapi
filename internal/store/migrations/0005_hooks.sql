-- 0005_hooks.sql: rule reactive hooks per instance pada app.db
CREATE TABLE hooks (
    instance_id TEXT    NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
    id          TEXT    NOT NULL,          -- slug [a-z0-9_-]{1,40}
    name        TEXT    NOT NULL DEFAULT '',
    enabled     INTEGER NOT NULL DEFAULT 1,
    priority    INTEGER NOT NULL DEFAULT 100, -- angka kecil dievaluasi lebih dulu
    trusted     INTEGER NOT NULL DEFAULT 0,   -- 1 = dibuat lewat control plane (boleh http ke localhost)
    spec        TEXT    NOT NULL,          -- JSON Rule
    version     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (instance_id, id)
);
