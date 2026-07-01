package org.hormigas.ws.infrastructure.persistance.postgres.attachment;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.hormigas.ws.ports.attachment.AttachmentManager;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@ApplicationScoped
public class AttachmentPostgresAdapter implements AttachmentManager {

    private static final String COLS =
            "id, conversation_id, uploader_id, object_key, file_name, content_type, size_bytes, status, created_at, confirmed_at";

    @Inject
    PgPool client;

    @Override
    public Uni<Void> insertPending(Attachment a) {
        Tuple t = Tuple.of(a.id(), a.conversationId(), a.uploaderId(), a.objectKey(), a.fileName(), a.contentType());
        t.addValue(a.sizeBytes());
        t.addValue(a.status().name());
        t.addValue(odt(a.createdAt()));
        return client.preparedQuery("""
                        INSERT INTO attachment (id, conversation_id, uploader_id, object_key, file_name,
                                                content_type, size_bytes, status, created_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""")
                .execute(t)
                .replaceWithVoid();
    }

    @Override
    public Uni<Attachment> findById(String id) {
        return client.preparedQuery("SELECT " + COLS + " FROM attachment WHERE id = $1")
                .execute(Tuple.of(id))
                .map(this::firstOrNull);
    }

    @Override
    public Uni<Attachment> markConfirmed(String id, Instant confirmedAt) {
        // Monotonic: only PENDING→CONFIRMED. Guarded so it can never regress a DELIVERED row back to
        // CONFIRMED. Returns the updated row (null if the row was not PENDING).
        return client.preparedQuery(
                        "UPDATE attachment SET status = 'CONFIRMED', confirmed_at = COALESCE(confirmed_at, $2) "
                                + "WHERE id = $1 AND status = 'PENDING' RETURNING " + COLS)
                .execute(Tuple.of(id, odt(confirmedAt)))
                .map(this::firstOrNull);
    }

    @Override
    public Uni<Void> markDelivered(String id) {
        // Monotonic CONFIRMED→DELIVERED (idempotent; a no-op if not currently CONFIRMED).
        return client.preparedQuery("UPDATE attachment SET status = 'DELIVERED' WHERE id = $1 AND status = 'CONFIRMED'")
                .execute(Tuple.of(id))
                .replaceWithVoid();
    }

    @Override
    public Uni<List<Attachment>> findStalePending(Instant cutoff, int limit) {
        return client.preparedQuery("""
                        SELECT %s FROM attachment
                         WHERE status = 'PENDING' AND created_at < $1
                         ORDER BY created_at
                         LIMIT $2""".formatted(COLS))
                .execute(Tuple.of(odt(cutoff), limit))
                .map(rs -> {
                    List<Attachment> out = new ArrayList<>();
                    rs.forEach(row -> out.add(map(row)));
                    return out;
                });
    }

    @Override
    public Uni<Void> markOrphaned(String id) {
        return client.preparedQuery("UPDATE attachment SET status = 'ORPHANED' WHERE id = $1")
                .execute(Tuple.of(id))
                .replaceWithVoid();
    }

    private Attachment firstOrNull(RowSet<Row> rs) {
        var it = rs.iterator();
        return it.hasNext() ? map(it.next()) : null;
    }

    private Attachment map(Row r) {
        OffsetDateTime created = r.getOffsetDateTime("created_at");
        OffsetDateTime confirmed = r.getOffsetDateTime("confirmed_at");
        return new Attachment(
                r.getString("id"),
                r.getString("conversation_id"),
                r.getString("uploader_id"),
                r.getString("object_key"),
                r.getString("file_name"),
                r.getString("content_type"),
                r.getLong("size_bytes"),
                AttachmentStatus.valueOf(r.getString("status")),
                created == null ? null : created.toInstant(),
                confirmed == null ? null : confirmed.toInstant());
    }

    private static OffsetDateTime odt(Instant i) {
        return i == null ? null : i.atOffset(ZoneOffset.UTC);
    }
}
