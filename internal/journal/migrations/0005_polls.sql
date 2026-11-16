-- 0005_polls.sql: definisi polling dan suara terakhir setiap pemilih.
CREATE TABLE IF NOT EXISTS polls (
    instance_id TEXT    NOT NULL,
    chat        TEXT    NOT NULL,
    wa_id       TEXT    NOT NULL,
    creator     TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    options     TEXT    NOT NULL,
    selectable  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (instance_id, chat, wa_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS poll_votes (
    instance_id TEXT    NOT NULL,
    chat        TEXT    NOT NULL,
    wa_id       TEXT    NOT NULL,
    voter       TEXT    NOT NULL,
    selected    TEXT    NOT NULL,
    voted_at    INTEGER NOT NULL,
    PRIMARY KEY (instance_id, chat, wa_id, voter)
) WITHOUT ROWID;
