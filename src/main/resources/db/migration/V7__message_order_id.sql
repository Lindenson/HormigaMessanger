-- V7__message_order_id.sql
-- Queryable order reference for message-level, order-scoped freeze (UC-U22, decisions #3/#4:
-- freeze is message-level scoped by orderId — there is NO chat-level freeze). orderId travels as
-- opaque message metadata (UC-H03); we extract just this one sanctioned key into a column so a
-- contract on one order freezes only that order's messages, not the whole (order-agnostic) chat.
ALTER TABLE message_history ADD COLUMN order_id VARCHAR(128);
CREATE INDEX idx_mh_conversation_order ON message_history(conversation_id, order_id);
