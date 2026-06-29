package org.hormigas.ws.infrastructure.messaging.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.ports.message.MessageModeration;

import java.util.Map;

/**
 * Order-event inbound adapter (the second driving adapter over the universal create-chat and freeze
 * use cases; the first is {@link org.hormigas.ws.infrastructure.rest.conversation.ChatResource}).
 * <b>Dual-driven principle</b> (concept §2): each event-capable operation is one core use case
 * exposed via interchangeable adapters — REST and this event consumer both invoke the SAME core op.
 *
 * <p>Consumes topic {@code order.events} (channel {@code order-events-in}) and maps two event types:
 * <ul>
 *   <li><b>UC-H01</b> — "a master expressed interest in an order" → {@link Chats#createChat}
 *       for the {@code (clientId, masterId)} pair, idempotent, orderId carried as metadata.</li>
 *   <li><b>UC-H04</b> — "a contract was reached" → resolve the chat by pair, then
 *       {@link MessageModeration#freezeByOrder} (message-level freeze scoped by orderId).</li>
 * </ul>
 * All other order events are ignored. Delivery is at-least-once; both handlers are idempotent.
 * A successful {@link Uni} acks the message; a failed one nacks → Kafka redelivery. Unparseable /
 * non-actionable messages are logged and acked (they would otherwise poison the partition — a proper
 * dead-letter policy is tracked as future ops hardening).
 */
@Slf4j
@ApplicationScoped
public class OrderEventConsumer {

    static final String META_ORDER_ID = "orderId";
    static final String FIELD_CLIENT_ID = "clientId";
    static final String FIELD_MASTER_ID = "masterId";

    @Inject
    Chats conversations;

    @Inject
    MessageModeration moderation;

    @Inject
    ObjectMapper mapper;

    @ConfigProperty(name = "messenger.order-events.type.master-interested", defaultValue = "order.master.interested")
    String typeMasterInterested;

    @ConfigProperty(name = "messenger.order-events.type.contract-reached", defaultValue = "order.contract.concluded")
    String typeContractReached;

    @Incoming("order-events-in")
    public Uni<Void> consume(String json) {
        KafkaEventEnvelope env;
        try {
            env = mapper.readValue(json, KafkaEventEnvelope.class);
        } catch (Exception e) {
            // Poison / unparseable — ack so it doesn't block the partition (dead-letter is future ops work).
            log.error("Dropping unparseable order.events message: {}", e.getMessage());
            return Uni.createFrom().voidItem();
        }
        if (env == null || env.eventType() == null) {
            log.warn("Dropping order.events message with no eventType (eventId={})",
                    env == null ? null : env.eventId());
            return Uni.createFrom().voidItem();
        }
        String type = env.eventType();
        if (type.equals(typeMasterInterested)) {
            return onMasterInterested(env);
        }
        if (type.equals(typeContractReached)) {
            return onContractReached(env);
        }
        log.debug("Ignoring unrelated order event type {} (eventId={})", type, env.eventId());
        return Uni.createFrom().voidItem();
    }

    /** UC-H01: master expressed interest → create the (client, master) chat, idempotent. */
    private Uni<Void> onMasterInterested(KafkaEventEnvelope env) {
        String clientId = env.payload(FIELD_CLIENT_ID);
        String masterId = env.payload(FIELD_MASTER_ID);
        String orderId = env.payload(META_ORDER_ID);
        if (clientId == null || masterId == null) {
            log.warn("master-interested event missing clientId/masterId (eventId={}) — skipping", env.eventId());
            return Uni.createFrom().voidItem();
        }
        Map<String, String> metadata = orderId == null ? Map.of() : Map.of(META_ORDER_ID, orderId);
        return conversations.createChat(clientId, masterId, metadata)
                .invoke(r -> log.info("order.events master-interested → chat {} ({}) for pair ({},{}) order {}",
                        r.conversation().id(), r.created() ? "created" : "existing", clientId, masterId, orderId))
                .replaceWithVoid();
    }

    /** UC-H04: contract reached → freeze that order's messages in the pair's chat, idempotent. */
    private Uni<Void> onContractReached(KafkaEventEnvelope env) {
        String clientId = env.payload(FIELD_CLIENT_ID);
        String masterId = env.payload(FIELD_MASTER_ID);
        String orderId = env.payload(META_ORDER_ID);
        if (clientId == null || masterId == null || orderId == null) {
            log.warn("contract-reached event missing clientId/masterId/orderId (eventId={}) — skipping", env.eventId());
            return Uni.createFrom().voidItem();
        }
        return conversations.findByPair(clientId, masterId)
                .flatMap(conv -> {
                    if (conv == null) {
                        // No chat for the pair → no messages to freeze. Ack (don't poison the partition).
                        log.warn("contract-reached for unknown chat pair ({},{}) order {} — nothing to freeze",
                                clientId, masterId, orderId);
                        return Uni.createFrom().<Void>voidItem();
                    }
                    return moderation.freezeByOrder(conv.id(), orderId)
                            .invoke(n -> log.info("order.events contract-reached → froze {} message(s) in chat {} order {}",
                                    n, conv.id(), orderId))
                            .replaceWithVoid();
                });
    }
}
