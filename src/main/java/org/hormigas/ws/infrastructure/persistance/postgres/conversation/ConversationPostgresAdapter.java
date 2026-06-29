package org.hormigas.ws.infrastructure.persistance.postgres.conversation;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationManager;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@ApplicationScoped
public class ConversationPostgresAdapter implements ConversationManager {

    private static final String COLS =
            "id, client_id, master_id, metadata_json, client_blocked, master_blocked, created_at, updated_at";

    @Inject
    PgPool client;

    @Override
    public Uni<Conversation> findByPair(String clientId, String masterId) {
        return client.preparedQuery(
                        "SELECT " + COLS + " FROM conversation WHERE client_id = $1 AND master_id = $2")
                .execute(Tuple.of(clientId, masterId))
                .map(rs -> firstOrNull(rs));
    }

    @Override
    public Uni<Conversation> findById(String id) {
        return client.preparedQuery("SELECT " + COLS + " FROM conversation WHERE id = $1")
                .execute(Tuple.of(id))
                .map(rs -> firstOrNull(rs));
    }

    @Override
    public Uni<List<Conversation>> findByParticipant(String userId) {
        return client.preparedQuery("""
                        SELECT %s FROM conversation
                         WHERE (client_id = $1 OR master_id = $1)
                           AND NOT (client_id = $1 AND client_hidden)
                           AND NOT (master_id = $1 AND master_hidden)
                         ORDER BY updated_at DESC""".formatted(COLS))
                .execute(Tuple.of(userId))
                .map(rs -> {
                    List<Conversation> out = new ArrayList<>();
                    rs.forEach(row -> out.add(map(row)));
                    return out;
                });
    }

    @Override
    public Uni<Void> hideFor(String chatId, String userId) {
        return client.preparedQuery("""
                        UPDATE conversation SET
                            client_hidden = CASE WHEN client_id = $2 THEN TRUE ELSE client_hidden END,
                            master_hidden = CASE WHEN master_id = $2 THEN TRUE ELSE master_hidden END,
                            updated_at = now()
                         WHERE id = $1""")
                .execute(Tuple.of(chatId, userId))
                .replaceWithVoid();
    }

    @Override
    public Uni<Void> setBlocked(String chatId, String userId, boolean blocked) {
        return client.preparedQuery("""
                        UPDATE conversation SET
                            client_blocked = CASE WHEN client_id = $2 THEN $3 ELSE client_blocked END,
                            master_blocked = CASE WHEN master_id = $2 THEN $3 ELSE master_blocked END,
                            updated_at = now()
                         WHERE id = $1""")
                .execute(Tuple.of(chatId, userId, blocked))
                .replaceWithVoid();
    }

    @Override
    public Uni<Conversation> insertIfAbsent(Conversation c) {
        return client.preparedQuery("""
                        INSERT INTO conversation (id, client_id, master_id, metadata_json)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (client_id, master_id) DO NOTHING
                        RETURNING\s""" + COLS)
                .execute(Tuple.of(c.id(), c.clientId(), c.masterId(), toJson(c.metadata())))
                .flatMap(rs -> {
                    Conversation inserted = firstOrNull(rs);
                    // ON CONFLICT DO NOTHING → no row → an equal pair already exists; return it.
                    return inserted != null
                            ? Uni.createFrom().item(inserted)
                            : findByPair(c.clientId(), c.masterId());
                });
    }

    // ── mapping ──────────────────────────────────────────────────────────────

    private Conversation firstOrNull(RowSet<Row> rs) {
        var it = rs.iterator();
        return it.hasNext() ? map(it.next()) : null;
    }

    private Conversation map(Row row) {
        return new Conversation(
                row.getString("id"),
                row.getString("client_id"),
                row.getString("master_id"),
                fromJson(row.getJsonObject("metadata_json")),
                row.getBoolean("client_blocked"),
                row.getBoolean("master_blocked"),
                row.getOffsetDateTime("created_at").toInstant(),
                row.getOffsetDateTime("updated_at").toInstant()
        );
    }

    private JsonObject toJson(Map<String, String> meta) {
        JsonObject jo = new JsonObject();
        if (meta != null) meta.forEach(jo::put);
        return jo;
    }

    private Map<String, String> fromJson(JsonObject jo) {
        Map<String, String> out = new LinkedHashMap<>();
        if (jo != null) {
            jo.getMap().forEach((k, v) -> out.put(k, v == null ? null : String.valueOf(v)));
        }
        return out;
    }
}
