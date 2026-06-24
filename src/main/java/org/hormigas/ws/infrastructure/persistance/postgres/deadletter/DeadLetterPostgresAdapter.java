package org.hormigas.ws.infrastructure.persistance.postgres.deadletter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;

import java.util.List;

/**
 * Postgres adapter for the {@code dead_letter} table (ADR-014). Mirrors the outbox envelope; payload
 * and meta are serialised to JSON (bound as text to JSONB, the same convention as the outbox).
 */
@Slf4j
@ApplicationScoped
public class DeadLetterPostgresAdapter implements DeadLetterStore<Message> {

    @Inject
    PgPool client;

    @Inject
    ObjectMapper mapper;

    @Override
    public Uni<Void> recordDraft(Message m) {
        Tuple t = Tuple.of(m.getMessageId(), m.getType().name(), m.getSenderId(), m.getRecipientId(), m.getConversationId());
        t.addValue(serialize(m.getPayload()));
        t.addValue(serialize(m.getMeta()));
        return client.preparedQuery("""
                        INSERT INTO dead_letter
                            (message_id, type, sender_id, recipient_id, conversation_id, payload_json, meta_json, status)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,'DRAFT')""")
                .execute(t)
                .replaceWithVoid()
                .onFailure().invoke(e -> log.error("Failed to record dead-letter draft for {}: {}",
                        m.getMessageId(), e.getMessage()));
    }

    @Override
    public Uni<Integer> deleteDrafts(List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return Uni.createFrom().item(0);
        }
        return client.preparedQuery("DELETE FROM dead_letter WHERE message_id = ANY($1)")
                .execute(Tuple.of(messageIds.toArray(new String[0])))
                .map(SqlResult::rowCount);
    }

    private String serialize(Object value) {
        if (value == null) return null;
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            log.warn("Could not serialize dead-letter field: {}", e.getMessage());
            return null;
        }
    }
}
