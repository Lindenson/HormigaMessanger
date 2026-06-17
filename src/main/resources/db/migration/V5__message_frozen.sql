-- V5__message_frozen.sql
-- Per-message freeze flag. A frozen message is immutable history (a contract was reached) and
-- cannot be deleted; non-frozen messages may be deleted by a participant (UC-U21/U22).
ALTER TABLE message_history ADD COLUMN frozen BOOLEAN NOT NULL DEFAULT FALSE;
