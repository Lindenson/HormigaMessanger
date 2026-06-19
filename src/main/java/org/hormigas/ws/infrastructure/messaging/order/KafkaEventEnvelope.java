package org.hormigas.ws.infrastructure.messaging.order;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * Inbound inter-service event envelope shipped by the Order service to topic {@code order.events}
 * (ADR-007, platform Kafka-first integration). JSON shape:
 *
 * <pre>
 *   { "eventId": "...", "eventType": "...", "occurredAt": "...",
 *     "payload": { "clientId": "...", "masterId": "...", "orderId": "..." } }
 * </pre>
 *
 * <p><b>ASSUMED CONTRACT — must reconcile with the Order team.</b> The envelope (eventId/eventType/
 * occurredAt/payload) mirrors ADR-007; the concrete {@code eventType} strings and the payload id
 * fields are config-driven ({@code messenger.order-events.*}) so reconciliation is a config change,
 * not a code change. {@code clientId}/{@code masterId} MUST be Ory identity ids (the same identities
 * used as conversation participants); {@code orderId} is opaque order metadata (UC-H03).
 *
 * <p>{@code eventId} is the producer-side dedup key (at-least-once delivery, {@code event_id} UNIQUE).
 * This consumer is additionally idempotent at the core-op level (create-by-pair, freeze-by-order),
 * so duplicate redelivery is harmless — no consumer-side dedup table is required (ADR-007).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KafkaEventEnvelope(
        String eventId,
        String eventType,
        String occurredAt,
        Map<String, String> payload) {

    /** Null-safe payload field accessor. */
    public String payload(String key) {
        return payload == null ? null : payload.get(key);
    }
}
