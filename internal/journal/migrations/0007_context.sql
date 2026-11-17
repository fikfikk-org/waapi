-- 0007_context.sql: context store opsional (riwayat terbatas per chat + indeks FTS5)
CREATE TABLE messages (
    id          INTEGER PRIMARY KEY,
    instance_id TEXT    NOT NULL,
    chat        TEXT    NOT NULL,
    wa_id       TEXT    NOT NULL,
    sender      TEXT    NOT NULL,
    from_me     INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    type        TEXT    NOT NULL,
    text        TEXT    NOT NULL DEFAULT '',   -- teks atau caption, maksimal 4096 karakter
    meta        BLOB,                           -- JSON ringkas: quoted_id, media_kind, filename, origin
    UNIQUE (instance_id, chat, wa_id)
);
CREATE INDEX ix_messages_chat_ts ON messages(instance_id, chat, ts);

CREATE TABLE chats (
    instance_id  TEXT    NOT NULL,
    chat         TEXT    NOT NULL,
    name         TEXT    NOT NULL DEFAULT '',
    last_ts      INTEGER NOT NULL,
    last_preview TEXT    NOT NULL DEFAULT '',  -- 120 karakter pertama
    unread       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, chat)
) WITHOUT ROWID;
CREATE INDEX ix_chats_last ON chats(instance_id, last_ts);

CREATE VIRTUAL TABLE messages_fts USING fts5(text, content='messages', content_rowid='id', tokenize='unicode61 remove_diacritics 2');
CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;
CREATE TRIGGER messages_au AFTER UPDATE OF text ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
