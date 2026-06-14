package org.hormigas.ws.infrastructure.persistance.postgres.history;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.infrastructure.persistance.postgres.HistoryRepository;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@ApplicationScoped
public class HistoryPostgresRepository implements HistoryRepository {

    @Inject
    PgPool client;

    // ----------------------------------------------------------------------
    // insertHistoryBatch
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Inserted>> insertHistoryBatch(List<HistoryRow> batch) {
        if (batch == null || batch.isEmpty()) return Uni.createFrom().item(Collections.emptyList());

        StringBuilder sql = new StringBuilder("""
                INSERT INTO message_history
                 (message_id, conversation_id, sender_id, recipient_id, payload_json, created_at)
                VALUES
                """);

        List<Object> flat = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            HistoryRow h = batch.get(i);
            if (i > 0) sql.append(",");
            int p = flat.size() + 1;
            sql.append("(")
                    .append("$").append(p).append(",")     // message_id
                    .append("$").append(p + 1).append(",") // conversation_id
                    .append("$").append(p + 2).append(",") // sender_id
                    .append("$").append(p + 3).append(",") // recipient_id
                    .append("$").append(p + 4).append("::jsonb,") // payload_json
                    .append("$").append(p + 5)             // created_at
                    .append(")");
            flat.add(h.messageId());
            flat.add(h.conversationId());
            flat.add(h.senderId());
            flat.add(h.recipientId());
            flat.add(h.payloadJson());
            flat.add(h.createdAt().atOffset(ZoneOffset.UTC));
        }

        sql.append(" RETURNING id, message_id");

        Tuple params = Tuple.tuple();
        flat.forEach(params::addValue);

        return client.preparedQuery(sql.toString())
                .execute(params)
                .onItem()
                .transform(rows -> {
                    List<Inserted> out = new ArrayList<>();
                    for (Row r : rows) {
                        out.add(new Inserted(r.getLong("id"), r.getString("message_id")));
                    }
                    return out;
                });
    }

    // ----------------------------------------------------------------------
    // deleteByIds
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> deleteByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return Uni.createFrom().item(0);
        String sql = "DELETE FROM message_history WHERE id = ANY($1)";
        return client.preparedQuery(sql)
                .execute(Tuple.of(toLongArray(ids)))
                .onItem().transform(SqlResult::rowCount);
    }

    // ----------------------------------------------------------------------
    // deleteByMessageIds
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> deleteByMessageIds(List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) return Uni.createFrom().item(0);
        String sql = "DELETE FROM message_history WHERE message_id = ANY($1)";
        return client.preparedQuery(sql)
                .execute(Tuple.of(toStringArray(messageIds)))
                .onItem().transform(SqlResult::rowCount);
    }

    // ----------------------------------------------------------------------
    // deleteOlderThan
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> deleteOlderThan(Instant threshold) {
        String sql = "DELETE FROM message_history WHERE created_at < $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(threshold.atOffset(ZoneOffset.UTC)))
                .onItem().transform(SqlResult::rowCount);
    }

    // ----------------------------------------------------------------------
    // findAllByConversationId
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<HistoryRow>> findAllByConversationId(String conversationId) {
        String sql = "SELECT * FROM message_history WHERE conversation_id = $1 ORDER BY created_at ASC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(conversationId))
                .onItem().transform(rows -> mapToHistoryRows(rows.iterator()));
    }

    // ----------------------------------------------------------------------
    // findAllBySenderId
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<HistoryRow>> findAllBySenderId(String senderId) {
        String sql = "SELECT * FROM message_history WHERE sender_id = $1 ORDER BY created_at ASC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(senderId))
                .onItem().transform(rows -> mapToHistoryRows(rows.iterator()));
    }

    // ----------------------------------------------------------------------
    // findAllByRecipientId
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<HistoryRow>> findAllByRecipientId(String recipientId) {
        String sql = "SELECT * FROM message_history WHERE recipient_id = $1 ORDER BY created_at ASC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(recipientId))
                .onItem().transform(rows -> mapToHistoryRows(rows.iterator()));
    }

    // ----------------------------------------------------------------------
    // findAllByParticipantId (sender OR recipient)
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<HistoryRow>> findAllByParticipantId(String participantId) {
        String sql = "SELECT * FROM message_history WHERE sender_id = $1 OR recipient_id = $1 ORDER BY created_at ASC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(participantId))
                .onItem().transform(rows -> mapToHistoryRows(rows.iterator()));
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------
    private static Long[] toLongArray(List<Long> ids) {
        return ids.toArray(new Long[0]);
    }

    private static String[] toStringArray(List<String> ids) {
        return ids.toArray(new String[0]);
    }

    private static List<HistoryRow> mapToHistoryRows(RowIterator<Row> rows) {
        List<HistoryRow> list = new ArrayList<>();
        while (rows.hasNext()) {
            Row r = rows.next();
            list.add(new HistoryRow(
                    r.getString("message_id"),
                    r.getString("conversation_id"),
                    r.getString("sender_id"),
                    r.getString("recipient_id"),
                    r.getString("payload_json"),
                    r.getOffsetDateTime("created_at").toInstant()
            ));
        }
        return list;
    }
}
