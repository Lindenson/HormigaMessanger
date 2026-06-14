package org.hormigas.ws.infrastructure.persistance.postgres.dto;

import java.time.Instant;

public record OutboxRow(
        long id,
        String senderId,
        String recipientId,
        String conversationId,
        String messageId,
        String correlationId,
        long senderTs,
        String senderTz,
        long serverTs,
        String type,
        String payloadJson,
        String metaJson,
        Instant createdAt,
        Instant leaseUntil
) {}