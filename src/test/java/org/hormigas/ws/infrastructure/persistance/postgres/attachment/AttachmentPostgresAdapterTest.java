package org.hormigas.ws.infrastructure.persistance.postgres.attachment;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link AttachmentPostgresAdapter} against real Postgres — focuses on the
 * monotonic status transitions PENDING → CONFIRMED → DELIVERED that make confirm retry-safe (4b).
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AttachmentPostgresAdapterTest {

    @Inject PgPool client;
    @Inject AttachmentPostgresAdapter adapter;

    @BeforeAll
    void schema() {
        client.query("""
                CREATE TABLE IF NOT EXISTS attachment (
                    id VARCHAR(64) PRIMARY KEY,
                    conversation_id VARCHAR(64) NOT NULL,
                    uploader_id VARCHAR(128) NOT NULL,
                    object_key VARCHAR(512) NOT NULL UNIQUE,
                    file_name VARCHAR(512),
                    content_type VARCHAR(255),
                    size_bytes BIGINT,
                    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    confirmed_at TIMESTAMPTZ
                );
                """).execute().await().indefinitely();
    }

    @BeforeEach
    void clean() {
        client.query("TRUNCATE attachment").execute().await().indefinitely();
    }

    private void insertPending(String id) {
        adapter.insertPending(new Attachment(id, "conv", "uploader", "conv/" + id, "f.png", "image/png", 10L,
                AttachmentStatus.PENDING, Instant.now(), null)).await().indefinitely();
    }

    private AttachmentStatus status(String id) {
        return adapter.findById(id).await().indefinitely().status();
    }

    @Test
    void lifecycle_pending_confirmed_delivered() {
        insertPending("a1");
        assertEquals(AttachmentStatus.PENDING, status("a1"));

        Attachment confirmed = adapter.markConfirmed("a1", Instant.now()).await().indefinitely();
        assertNotNull(confirmed, "markConfirmed returns the updated row for a PENDING attachment");
        assertEquals(AttachmentStatus.CONFIRMED, confirmed.status());
        assertNotNull(confirmed.confirmedAt());

        adapter.markDelivered("a1").await().indefinitely();
        assertEquals(AttachmentStatus.DELIVERED, status("a1"));
    }

    @Test
    void markConfirmed_is_guarded_and_never_regresses_a_delivered_row() {
        insertPending("a1");
        adapter.markConfirmed("a1", Instant.now()).await().indefinitely();
        adapter.markDelivered("a1").await().indefinitely();

        // a late/duplicate confirm must NOT flip DELIVERED back to CONFIRMED
        Attachment again = adapter.markConfirmed("a1", Instant.now()).await().indefinitely();
        assertNull(again, "markConfirmed is a no-op (returns null) when the row is not PENDING");
        assertEquals(AttachmentStatus.DELIVERED, status("a1"));
    }

    @Test
    void markDelivered_only_advances_from_confirmed() {
        insertPending("a1");
        // PENDING → markDelivered is a no-op (guarded to CONFIRMED)
        adapter.markDelivered("a1").await().indefinitely();
        assertEquals(AttachmentStatus.PENDING, status("a1"));
    }

    @Test
    void findStalePending_excludes_confirmed_and_delivered() {
        insertPending("p1");
        insertPending("c1");
        adapter.markConfirmed("c1", Instant.now()).await().indefinitely();

        var stale = adapter.findStalePending(Instant.now().plusSeconds(60), 100).await().indefinitely();
        assertEquals(1, stale.size(), "only the PENDING row is a reclaim candidate");
        assertEquals("p1", stale.get(0).id());
    }
}
