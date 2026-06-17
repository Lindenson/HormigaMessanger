package org.hormigas.ws.infrastructure.persistance.postgres.message;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.ports.message.ReadReceipts;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@ApplicationScoped
public class ReadReceiptsAdapter implements ReadReceipts {

    @Inject
    PgPool client;

    @Override
    public Uni<Integer> markDelivered(String messageId) {
        return client.preparedQuery("""
                        UPDATE message_history SET status = 'DELIVERED'
                         WHERE message_id = $1 AND status = 'SENT'
                        """)
                .execute(Tuple.of(messageId))
                .map(r -> r.rowCount());
    }

    @Override
    public Uni<Integer> markRead(String conversationId, String readerId) {
        return client.preparedQuery("""
                        UPDATE message_history SET status = 'READ'
                         WHERE conversation_id = $1 AND recipient_id = $2 AND status <> 'READ'
                        """)
                .execute(Tuple.of(conversationId, readerId))
                .map(r -> r.rowCount());
    }

    @Override
    public Uni<List<Receipt>> receipts(String conversationId) {
        return client.preparedQuery(
                        "SELECT message_id, status FROM message_history WHERE conversation_id = $1 ORDER BY created_at ASC")
                .execute(Tuple.of(conversationId))
                .map(rs -> {
                    List<Receipt> out = new ArrayList<>();
                    rs.forEach(row -> out.add(new Receipt(row.getString("message_id"), row.getString("status"))));
                    return out;
                });
    }
}
