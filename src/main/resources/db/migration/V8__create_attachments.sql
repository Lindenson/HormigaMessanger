-- V8__create_attachments.sql
-- Two-phase presigned-upload attachments (concept §10, ADR-010 pattern):
--   (1) upload-url: persist a PENDING row + issue a presigned PUT URL;
--   (2) client uploads bytes DIRECTLY to MinIO (never through the service);
--   (3) confirm: verify the object exists → mark CONFIRMED → emit a persistent chat message
--       referencing the objectKey (binaries never transit the service; we store the key, not a URL).
-- A PENDING row whose confirm never arrives is reclaimed by AttachmentCleanupScheduler (→ ORPHANED
-- + MinIO object deleted). status: PENDING | CONFIRMED | ORPHANED.
CREATE TABLE attachment (
    id              VARCHAR(64)  PRIMARY KEY,
    conversation_id VARCHAR(64)  NOT NULL,
    uploader_id     VARCHAR(128) NOT NULL,
    object_key      VARCHAR(512) NOT NULL UNIQUE,
    file_name       VARCHAR(512),
    content_type    VARCHAR(255),
    size_bytes      BIGINT,
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    confirmed_at    TIMESTAMPTZ
);

-- Cleanup scan: find stale PENDING rows by age.
CREATE INDEX idx_attachment_status_created ON attachment(status, created_at);
