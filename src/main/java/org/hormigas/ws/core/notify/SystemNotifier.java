package org.hormigas.ws.core.notify;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.util.Map;

/**
 * Emits a must-arrive system notice (Strategy C, ADR-014). Trigger-agnostic core op (REST adapter
 * today; an event adapter could be added — dual-driven). Eager-draft model: record the durable
 * {@code dead_letter} DRAFT first (so nothing can be silently lost), then enqueue the transient
 * outbox row that the poller delivers as {@code SYSTEM_OUT}. The recipient's {@code SYSTEM_ACK}
 * retracts the draft.
 */
@ApplicationScoped
public class SystemNotifier implements Notices {

    @Inject
    OutboxManager<Message> outbox;

    @Inject
    DeadLetterStore<Message> deadLetter;

    @Inject
    IdGenerator idGenerator;

    /**
     * Notify {@code recipientId} with a system payload. {@code conversationId} is optional — a
     * notice may be addressed to a participant outside any chat (synthetic {@code system:<id>}).
     */
    public Uni<Void> notify(String recipientId, String kind, String body,
                            Map<String, String> meta, String conversationId) {
        long now = System.currentTimeMillis();
        Message notice = Message.builder()
                .type(MessageType.SYSTEM_OUT)
                .messageId(idGenerator.generateId())
                .senderId("server")
                .recipientId(recipientId)
                .conversationId(conversationId != null ? conversationId : "system:" + recipientId)
                .senderTimestamp(now)
                .senderTimezone("UTC")
                .serverTimestamp(now)
                .payload(Message.Payload.builder()
                        .kind(kind != null && !kind.isBlank() ? kind : "event")
                        .body(body)
                        .build())
                .meta(meta)
                .build();

        // Durable record FIRST (conservative: a crash before enqueue shows it as undelivered),
        // then the transient delivery vehicle.
        return deadLetter.recordDraft(notice)
                .replaceWith(outbox.saveTransient(notice))
                .replaceWithVoid();
    }
}
