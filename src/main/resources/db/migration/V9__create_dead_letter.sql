-- V9__create_dead_letter.sql
-- Dead-letter record for Strategy C (must-arrive system notices) — ADR-014, eager-draft model.
-- On a C send we insert BOTH the outbox row (transient delivery vehicle) AND a dead_letter row with
-- status='DRAFT' (the durable record, exists from t=0 so nothing can be silently lost). On SYSTEM_ACK
-- the messageId is added to a Redis confirmed-set; an independent cleanup sweep deletes the confirmed
-- drafts. What remains as DRAFT = the genuinely undelivered notices (the dead letters), queryable.
-- Envelope mirrors `outbox` (Kafka dead-letter-topic portable, ADR-013); no FK — independent table.
CREATE TABLE dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    message_id      VARCHAR(128) NOT NULL,
    type            VARCHAR(64)  NOT NULL,
    sender_id       VARCHAR(128) NOT NULL,
    recipient_id    VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(128),
    payload_json    JSONB,
    meta_json       JSONB,
    status          VARCHAR(16)  NOT NULL DEFAULT 'DRAFT',
    reason          VARCHAR(64),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- retract-by-id (cleanup sweep deletes confirmed drafts by message_id)
CREATE INDEX idx_dead_letter_message_id ON dead_letter(message_id);
-- ops query: list still-undelivered notices by age
CREATE INDEX idx_dead_letter_status_created ON dead_letter(status, created_at);
