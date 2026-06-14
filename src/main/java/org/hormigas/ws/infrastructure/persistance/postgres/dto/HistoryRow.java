package org.hormigas.ws.infrastructure.persistance.postgres.dto;

import java.time.Instant;

public record HistoryRow(
        String messageId,
        String conversationId,
        String senderId,
        String recipientId,
        String payloadJson,
        Instant createdAt
) {}