-- V6__message_status.sql
-- Per-message delivery/read status for receipts (UC-U13/U14): SENT (default) → DELIVERED → READ.
-- The recipient marks their received messages READ; the sender reads the status back.
ALTER TABLE message_history ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'SENT';
