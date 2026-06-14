

package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import jakarta.inject.Inject;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class RedisIdempotencyTest {

    @Inject
    RedisIdempotencyManager manager;

    @Inject
    ReactiveRedisDataSource redis;

    @Inject
    MessengerConfig config;

    ReactiveKeyCommands<String> keys;
    ReactiveValueCommands<String, Integer> values;

    @BeforeEach
    public void setup() {
        keys = redis.key();
        values = redis.value(Integer.class);
        redis.flushall().await().indefinitely();
    }

    private Message newMsg(UUID recipient, UUID msgId) {
        return Message.builder()
                .recipientId(recipient.toString())
                .messageId(msgId.toString())
                .serverTimestamp(Instant.now().toEpochMilli())
                .build();
    }

    // -------------------------------------------------------
    // BASIC OPERATIONS
    // -------------------------------------------------------

    @Test
    public void testAddStoresKey() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        StageResult<Message> result = manager.add(msg).await().indefinitely();
        assertTrue(result.isSuccess());

        Boolean exists = keys.exists("receiver:" + r + ":message:" + m).await().indefinitely();
        assertTrue(exists);
    }

    @Test
    public void testIsInProgressAfterAdd() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();
        Boolean inProgress = manager.isInProgress(msg).await().indefinitely();

        assertTrue(inProgress);
    }

    @Test
    public void testRemoveDeletesKey() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();

        StageResult<Message>  removed = manager.remove(msg).await().indefinitely();
        assertTrue(removed.isSuccess());

        Boolean exists = keys.exists("receiver:" + r + ":message:" + m).await().indefinitely();
        assertFalse(exists);
    }

    @Test
    public void testRemoveMissingKeyReturnsSkipped() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        StageResult<Message>  removed = manager.remove(msg).await().indefinitely();
        assertTrue(removed.isSkipped());
    }

    // -------------------------------------------------------
    // TTL BEHAVIOR
    // -------------------------------------------------------

    @Test
    public void testRepeatAddUpdatesTTL() throws InterruptedException {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();
        Thread.sleep(1000);

        long ttl1 = keys.ttl("receiver:" + r + ":message:" + m).await().indefinitely();

        manager.add(msg).await().indefinitely();
        long ttl2 = keys.ttl("receiver:" + r + ":message:" + m).await().indefinitely();

        assertTrue(ttl2 > ttl1);
    }

    @Test
    public void testTTLExpires() throws InterruptedException {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();

        Thread.sleep((config.idempotent().ttlSeconds() + 1) * 1000L);

        Boolean exists = manager.isInProgress(msg).await().indefinitely();
        assertFalse(exists);
    }

    @Test
    public void testTTLIsSet() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();

        long ttl = keys.ttl("receiver:" + r + ":message:" + m).await().indefinitely();
        assertTrue(ttl > 0);
        assertNotEquals(-1, ttl); // not persistent
    }

    // -------------------------------------------------------
    // VALUE INTEGRITY
    // -------------------------------------------------------

    @Test
    public void testDummyValueStoredCorrectly() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();

        Integer stored = values.get("receiver:" + r + ":message:" + m).await().indefinitely();
        assertEquals(1, stored);
    }

    @Test
    public void testKeyFormatIsCorrect() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        manager.add(msg).await().indefinitely();

        Boolean exists = keys.exists("receiver:" + msg.getRecipientId() + ":message:" + msg.getMessageId())
                .await().indefinitely();
        assertTrue(exists);
    }

    // -------------------------------------------------------
    // PARALLELITY
    // -------------------------------------------------------

    @Test
    public void testParallelAddAndCheck() {
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        Boolean inProgressBefore = manager.isInProgress(msg).await().indefinitely();
        assertFalse(inProgressBefore);

        StageResult<Message>  added = manager.add(msg).await().indefinitely();
        assertTrue(added.isSuccess());

        Boolean inProgressAfter = manager.isInProgress(msg).await().indefinitely();
        assertTrue(inProgressAfter);
    }

    // -------------------------------------------------------
    // FAILURE CASES
    // -------------------------------------------------------

    @Test
    public void testFailureDoesNotCrashAdd() {
        redis.execute("DEBUG", "SEGFAULT")
                .onFailure().recoverWithNull()
                .await().indefinitely();

        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        StageResult<Message>  st = manager.add(msg).await().indefinitely();
        assertNotNull(st);
    }

    @Test
    public void testFailureDoesNotCrashRemove() {
        redis.execute("DEBUG", "SEGFAULT")
                .onFailure().recoverWithNull()
                .await().indefinitely();

        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        StageResult<Message>  st = manager.remove(msg).await().indefinitely();
        assertTrue(st.isSkipped());
    }

    @Test
    public void testFailureDoesNotCrashIsInProgress() {
        redis.execute("DEBUG", "SEGFAULT")
                .onFailure().recoverWithNull()
                .await().indefinitely();

        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        Message msg = newMsg(r, m);

        Boolean inProgress = manager.isInProgress(msg).await().indefinitely();
        assertFalse(inProgress);
    }
}
