-- V2__create_outbox_and_history.sql

CREATE TABLE message_history (
                                 id BIGSERIAL PRIMARY KEY,

    -- core message fields (subset of domain Message)
                                 message_id      VARCHAR(128) NOT NULL,
                                 conversation_id VARCHAR(128) NOT NULL,
                                 sender_id       VARCHAR(128) NOT NULL,
                                 recipient_id    VARCHAR(128) NOT NULL,

                                 payload_json    JSONB NOT NULL,
    -- outbox control fields
                                 created_at      TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX idx_mh_conversation_id ON message_history(conversation_id);
CREATE INDEX idx_mh_sender_id ON message_history(sender_id);
CREATE INDEX idx_mh_recipient_id ON message_history(recipient_id);
CREATE INDEX idx_mh_sender_recipient ON message_history(sender_id, recipient_id);


CREATE TABLE outbox (
                        id BIGSERIAL PRIMARY KEY,

    -- core message fields (subset of domain Message)
                        type            VARCHAR(64) NOT NULL,
                        sender_id       VARCHAR(128) NOT NULL,
                        recipient_id    VARCHAR(128) NOT NULL,
                        conversation_id VARCHAR(128) NOT NULL,
                        message_id      VARCHAR(128) NOT NULL,
                        correlation_id  VARCHAR(128),
                        sender_ts       BIGINT NOT NULL,
                        sender_tz       VARCHAR(64),
                        server_ts       BIGINT NOT NULL,

                        payload_json    JSONB NOT NULL,
                        meta_json       JSONB,

    -- outbox control fields
                        created_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
                        lease_until     TIMESTAMPTZ DEFAULT now() + interval '2 seconds' NOT NULL,
                        processing_attempts INT DEFAULT 0 NOT NULL
);
CREATE INDEX idx_outbox_id_lease ON outbox(lease_until, id);
