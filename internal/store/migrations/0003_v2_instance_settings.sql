-- 0003_v2_instance_settings.sql: konfigurasi per instance (JSON bertipe) dan waktu pairing
ALTER TABLE instances ADD COLUMN settings TEXT NOT NULL DEFAULT '{}';
ALTER TABLE instances ADD COLUMN paired_at INTEGER;
-- Instance v1 yang sudah tertaut dianggap nomor lama (warm-up dihitung dari created_at)
UPDATE instances SET paired_at = created_at WHERE jid IS NOT NULL;
