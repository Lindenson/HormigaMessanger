package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.redis.client.RedisAPI;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.presence.OnlineClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class RedisPresenceManagerTest {

    @Inject
    RedisPresenceManager presence;

    @Inject
    RedisAPI redis;

    @BeforeEach
    public void clear() {
        redis.flushall(List.of()).await().indefinitely();
    }

    // --------------------------------------------------------
    // ADD + GET
    // --------------------------------------------------------

    @Test
    public void testAddAndGetAll() {
        presence.add("c1", "Alice", 100).await().indefinitely();
        presence.add("c2", "Bob", 200).await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();

        assertEquals(2, all.size());
        assertTrue(all.stream().anyMatch(c -> c.name().equals("Alice")));
        assertTrue(all.stream().anyMatch(c -> c.name().equals("Bob")));
    }

    // --------------------------------------------------------
    // REMOVE logic with timestamp threshold
    // --------------------------------------------------------

    @Test
    public void testRemoveDeletesWhenTimestampMatches() {
        presence.add("c1", "Alice", 100).await().indefinitely();

        presence.remove("c1", 100).await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        assertEquals(0, all.size());
    }

    @Test
    public void testRemoveDeletesWhenTimestampIsGreater() {
        presence.add("c1", "Alice", 100).await().indefinitely();

        presence.remove("c1", 200).await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        assertEquals(0, all.size());
    }

    @Test
    public void testRemoveDoesNotDeleteIfTimestampIsLower() {
        presence.add("c1", "Alice", 200).await().indefinitely();

        // Trying to delete with older timestamp
        presence.remove("c1", 100).await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        assertEquals(1, all.size());
    }

    @Test
    public void testDuplicateRemoveIsSafe() {
        presence.add("c1", "Alice", 100).await().indefinitely();

        presence.remove("c1", 100).await().indefinitely();
        presence.remove("c1", 100).await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        assertEquals(0, all.size());
    }

    // --------------------------------------------------------
    // MULTIPLE CLIENTS + SCAN
    // --------------------------------------------------------

    @Test
    public void testScanMultipleClients() {
        for (int i = 0; i < 150; i++) {
            presence.add("c" + i, "U" + i, i).await().indefinitely();
        }

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        assertEquals(150, all.size());
    }

    // --------------------------------------------------------
    // BAD DATA DOES NOT BREAK PARSER
    // --------------------------------------------------------

    @Test
    public void testBadStoredValueIsIgnoredGracefully() {
        redis.set(List.of("client:c1", "ThisIsNotValid"))
                .await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();

        // parseClientData returns null on bad value
        assertEquals(0, all.size());
    }

    @Test
    public void testPartialCorruptedValue() {
        redis.set(List.of("client:c1", "name:Alice:timestamp:NotANumber"))
                .await().indefinitely();

        List<OnlineClient> all = presence.getAll().await().indefinitely();
        // it logs and skips
        assertEquals(0, all.size());
    }

    // --------------------------------------------------------
    // REMOVE NULL-SAFE
    // --------------------------------------------------------

    @Test
    public void testRemoveNullDoesNothing() {
        presence.remove(null, 100).await().indefinitely();

        // Should not throw
        assertEquals(0, presence.getAll().await().indefinitely().size());
    }
}
