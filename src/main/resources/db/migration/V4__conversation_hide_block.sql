-- V4__conversation_hide_block.sql
-- Per-participant soft-delete (hide) + blacklist (block) flags. The chat itself is never
-- removed; a participant only hides it for themselves. Blocking is per-side.

ALTER TABLE conversation
    ADD COLUMN client_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN master_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN client_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN master_blocked BOOLEAN NOT NULL DEFAULT FALSE;
