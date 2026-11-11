-- Tambahkan event pairing ke instance yang masih memakai daftar event default lama
UPDATE instances
SET webhook_events = webhook_events || ',pairing'
WHERE webhook_events = 'message,receipt,connection';
