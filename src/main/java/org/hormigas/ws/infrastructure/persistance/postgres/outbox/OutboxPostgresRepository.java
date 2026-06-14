package org.hormigas.ws.infrastructure.persistance.postgres.outbox;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.data.Interval;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.infrastructure.persistance.postgres.OutboxRepository;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@ApplicationScoped
public class OutboxPostgresRepository implements OutboxRepository {

    @Inject
    PgPool client;

    // ----------------------------------------------------------------------
    // insertBatch (OUTBOX)
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Inserted>> insertOutboxBatch(List<OutboxMessage> batch) {
        if (batch == null || batch.isEmpty()) return Uni.createFrom().item(Collections.emptyList());

        StringBuilder sql = new StringBuilder(
                """
                INSERT INTO outbox
                 (type, sender_id, recipient_id,
                  conversation_id, message_id, correlation_id,
                  sender_ts, sender_tz, server_ts, payload_json, meta_json)
                VALUES
                """);

        List<Object> flat = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            OutboxMessage m = batch.get(i);
            if (i > 0) sql.append(",");

            int p = flat.size() + 1;

            sql.append("(")
                    .append("$").append(p).append(",")     // type
                    .append("$").append(p + 1).append(",") // sender_id
                    .append("$").append(p + 2).append(",") // recipient_id
                    .append("$").append(p + 3).append(",") // conversation_id
                    .append("$").append(p + 4).append(",") // message_id
                    .append("$").append(p + 5).append(",") // correlation_id
                    .append("$").append(p + 6).append(",") // sender_ts
                    .append("$").append(p + 7).append(",") // sender_tz
                    .append("$").append(p + 8).append(",") // server_ts
                    .append("$").append(p + 9).append(",") // payload_json
                    .append("$").append(p + 10)            // meta_json
                    .append(")");

            flat.add(m.type());
            flat.add(m.senderId());
            flat.add(m.recipientId());
            flat.add(m.conversationId());
            flat.add(m.messageId());
            flat.add(m.correlationId());
            flat.add(m.senderTimestamp());
            flat.add(m.senderTimezone());
            flat.add(m.serverTimestamp());
            flat.add(m.payloadJson());
            flat.add(m.metaJson());
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
    // fetchBatchForProcessing
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<OutboxRow>> fetchBatchForProcessing(int batchSize, Duration leaseDuration) {
        if (batchSize <= 0) batchSize = 50;

        Interval interval = durationToPgIntervalStrict(leaseDuration);

        String selectSql = """
            SELECT id FROM outbox
            WHERE lease_until <= now()
            ORDER BY id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """;

        String updateSql = """
            UPDATE outbox
            SET lease_until = now() + ($1::interval),
                processing_attempts = processing_attempts + 1
            WHERE id = ANY($2)
            RETURNING id, type,
                      sender_id, recipient_id, conversation_id, message_id,
                      correlation_id, sender_ts, sender_tz, server_ts,
                      payload_json, meta_json, created_at, lease_until
            """;

        int finalBatchSize = batchSize;
        return client.withTransaction(conn ->
                conn.preparedQuery(selectSql).execute(Tuple.of(finalBatchSize))
                        .flatMap(rows -> {
                            List<Long> ids = new ArrayList<>();
                            for (Row r : rows) ids.add(r.getLong("id"));

                            if (ids.isEmpty())
                                return Uni.createFrom().item(Collections.<OutboxRow>emptyList());

                            Tuple params = Tuple.of(interval, toLongArray(ids));

                            return conn.preparedQuery(updateSql).execute(params)
                                    .onItem().transform(updated -> {
                                        List<OutboxRow> list = new ArrayList<>();
                                        for (Row r : updated) {
                                            list.add(new OutboxRow(
                                                    r.getLong("id"),
                                                    r.getString("sender_id"),
                                                    r.getString("recipient_id"),
                                                    r.getString("conversation_id"),
                                                    r.getString("message_id"),
                                                    r.getString("correlation_id"),
                                                    r.getLong("sender_ts"),
                                                    r.getString("sender_tz"),
                                                    r.getLong("server_ts"),
                                                    r.getString("type"),
                                                    r.getString("payload_json"),
                                                    r.getString("meta_json"),
                                                    r.getOffsetDateTime("created_at").toInstant(),
                                                    r.getOffsetDateTime("lease_until").toInstant()
                                            ));
                                        }
                                        return list;
                                    });
                        })
        );
    }

    // ----------------------------------------------------------------------
    // insertHistoryAndOutboxTransactional
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Inserted>> insertHistoryAndOutboxTransactional(
            List<HistoryRow> historyRows,
            List<OutboxMessage> outboxBatch
    ) {
        return client.withTransaction(conn -> {

            Uni<Void> insertHistory = Uni.createFrom().voidItem();

            if (historyRows != null && !historyRows.isEmpty()) {

                StringBuilder hsql = new StringBuilder("""
                    INSERT INTO message_history
                    (message_id, conversation_id, sender_id, recipient_id,
                     payload_json, created_at)
                    VALUES
                    """);

                List<Object> flat = new ArrayList<>();

                for (int i = 0; i < historyRows.size(); i++) {
                    HistoryRow h = historyRows.get(i);

                    if (i > 0) hsql.append(",");

                    int p = flat.size() + 1;

                    hsql.append("(")
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

                hsql.append(" RETURNING id");

                Tuple params = Tuple.tuple();
                flat.forEach(params::addValue);

                insertHistory =
                        conn.preparedQuery(hsql.toString()).execute(params).replaceWithVoid();
            }

            return insertHistory
                    .flatMap(v -> insertBatchInTx(conn, outboxBatch))
                    .onFailure().invoke(er -> log.error("Error inserting history", er));
        });
    }

    // ----------------------------------------------------------------------
    // deleteProcessedByIds
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> deleteProcessedByIds(List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty())
            return Uni.createFrom().item(0);

        String sql = "DELETE FROM outbox WHERE message_id = ANY($1)";

        return client.preparedQuery(sql)
                .execute(Tuple.of(toStringArray(messageIds)))
                .onItem().transform(SqlResult::rowCount);
    }

    // ----------------------------------------------------------------------
    // deleteOlderThan
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> deleteOlderThan(long idThreshold) {
        String sql = "DELETE FROM outbox WHERE id < $1";

        return client.preparedQuery(sql)
                .execute(Tuple.of(idThreshold))
                .onItem().transform(SqlResult::rowCount);
    }

    // ----------------------------------------------------------------------
    // helpers
    // ----------------------------------------------------------------------
    private Uni<List<Inserted>> insertBatchInTx(SqlConnection conn, List<OutboxMessage> batch) {
        if (batch == null || batch.isEmpty()) return Uni.createFrom().item(Collections.emptyList());

        StringBuilder sql = new StringBuilder("""
        INSERT INTO outbox
         (type, sender_id, recipient_id,
          conversation_id, message_id, correlation_id,
          sender_ts, sender_tz, server_ts, payload_json, meta_json)
        VALUES
        """);

        List<Object> flat = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            OutboxMessage m = batch.get(i);
            if (i > 0) sql.append(",");

            int p = flat.size() + 1;

            sql.append("(")
                    .append("$").append(p).append(",")
                    .append("$").append(p + 1).append(",")
                    .append("$").append(p + 2).append(",")
                    .append("$").append(p + 3).append(",")
                    .append("$").append(p + 4).append(",")
                    .append("$").append(p + 5).append(",")
                    .append("$").append(p + 6).append(",")
                    .append("$").append(p + 7).append(",")
                    .append("$").append(p + 8).append(",")
                    .append("$").append(p + 9).append(",")
                    .append("$").append(p + 10)
                    .append(")");

            flat.add(m.type());
            flat.add(m.senderId());
            flat.add(m.recipientId());
            flat.add(m.conversationId());
            flat.add(m.messageId());
            flat.add(m.correlationId());
            flat.add(m.senderTimestamp());
            flat.add(m.senderTimezone());
            flat.add(m.serverTimestamp());
            flat.add(m.payloadJson());
            flat.add(m.metaJson());
        }

        sql.append(" RETURNING id, message_id");

        Tuple params = Tuple.tuple();
        flat.forEach(params::addValue);

        return conn.preparedQuery(sql.toString())
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

    private static Long[] toLongArray(List<Long> ids) {
        return ids.toArray(new Long[0]);
    }

    private static String[] toStringArray(List<String> ids) {
        return ids.toArray(new String[0]);
    }

    static Interval durationToPgIntervalStrict(Duration d) {
        if (d == null) {
            throw new IllegalArgumentException("leaseDuration must not be null");
        }

        Duration max = Duration.ofHours(24);
        if (d.compareTo(max) > 0) {
            throw new IllegalArgumentException("leaseDuration must be <= 24 hours");
        }

        if (d.isNegative() || d.isZero()) {
            d = Duration.ofSeconds(1);
        } else if (d.compareTo(Duration.ofSeconds(1)) < 0) {
            d = Duration.ofSeconds(1);
        }

        return Interval.of(d);
    }
}
