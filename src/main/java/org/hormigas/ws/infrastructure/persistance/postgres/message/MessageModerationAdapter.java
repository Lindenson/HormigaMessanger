package org.hormigas.ws.infrastructure.persistance.postgres.message;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.ports.message.MessageModeration;

@Slf4j
@ApplicationScoped
public class MessageModerationAdapter implements MessageModeration {

    @Inject
    PgPool client;

    @Override
    public Uni<DeleteOutcome> deleteMessage(String conversationId, String messageId) {
        return client.preparedQuery(
                        "SELECT frozen FROM message_history WHERE conversation_id = $1 AND message_id = $2")
                .execute(Tuple.of(conversationId, messageId))
                .flatMap(rs -> {
                    var it = rs.iterator();
                    if (!it.hasNext()) {
                        return Uni.createFrom().item(DeleteOutcome.NOT_FOUND);
                    }
                    if (it.next().getBoolean("frozen")) {
                        return Uni.createFrom().item(DeleteOutcome.FROZEN);
                    }
                    return client.preparedQuery(
                                    "DELETE FROM message_history WHERE conversation_id = $1 AND message_id = $2 AND frozen = FALSE")
                            .execute(Tuple.of(conversationId, messageId))
                            .map(r -> r.rowCount() > 0 ? DeleteOutcome.DELETED : DeleteOutcome.FROZEN);
                });
    }

    @Override
    public Uni<Integer> freezeConversation(String conversationId) {
        return client.preparedQuery(
                        "UPDATE message_history SET frozen = TRUE WHERE conversation_id = $1 AND frozen = FALSE")
                .execute(Tuple.of(conversationId))
                .map(r -> r.rowCount());
    }
}
