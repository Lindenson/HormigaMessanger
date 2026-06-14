package org.hormigas.ws.infrastructure.persistance.postgres.dto;


public record OutboxMessage(
        String type,
        String senderId,
        String recipientId,
        String conversationId,
        String messageId,
        String correlationId,
        long senderTimestamp,
        String senderTimezone,
        long serverTimestamp,
        String payloadJson,
        String metaJson
) {}