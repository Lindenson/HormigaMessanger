-- V3__create_conversation.sql
-- 1:1 master↔client chat, keyed by the participant pair (idempotent creation).

CREATE TABLE conversation (
    id            VARCHAR(128) PRIMARY KEY,
    client_id     VARCHAR(128) NOT NULL,
    master_id     VARCHAR(128) NOT NULL,
    metadata_json JSONB,
    created_at    TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT uq_conversation_pair UNIQUE (client_id, master_id)
);

CREATE INDEX idx_conversation_client ON conversation(client_id);
CREATE INDEX idx_conversation_master ON conversation(master_id);
