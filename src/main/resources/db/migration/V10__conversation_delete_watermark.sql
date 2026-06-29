-- V10__conversation_delete_watermark.sql
-- Replace the per-side boolean hide flags with a per-side DELETE WATERMARK: the messageId (ULID
-- cursor, same column type as message_history.message_id) up to which that participant deleted their
-- view of the chat. NULL = never deleted.
--
-- Semantics ("delete for me", per side):
--   * history read for a side is floored at its watermark (messages at/below it are hidden), unless
--     the caller asks includeDeleted;
--   * the chat drops from a side's list while no message exists ABOVE its watermark, and reappears
--     on the next message — so a new order revives the thread naturally, no reopen logic;
--   * deletion is NOT terminal for messaging (the peer keeps writing) — block remains the only
--     terminal stop;
--   * rows are never removed; the admin path reads everything.

ALTER TABLE conversation
    ADD COLUMN deleted_from_client VARCHAR(128),
    ADD COLUMN deleted_from_master VARCHAR(128);

ALTER TABLE conversation
    DROP COLUMN client_hidden,
    DROP COLUMN master_hidden;
