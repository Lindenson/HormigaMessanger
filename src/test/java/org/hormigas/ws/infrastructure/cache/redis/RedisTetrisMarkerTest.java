package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.redis.client.RedisAPI;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class RedisTetrisMarkerTest {

    @Inject
    RedisTetrisMarker tetris;

    @Inject
    RedisAPI redis;

    private String recipient1;
    private String recipient2;

    @BeforeEach
    public void setup() {
        recipient1 = UUID.randomUUID().toString();
        recipient2 = UUID.randomUUID().toString();
        redis.flushall(List.of()).await().indefinitely();
    }

    @Test
    public void testOnSentAndComputeSafeDeleteId() {
        Message message1 = msg(1L, recipient1);
        Message message2 = msg(2L, recipient1);
        tetris.onSent(message1).await().indefinitely();
        tetris.onSent(message2).await().indefinitely();

        Message message10 = msg(10L, recipient2);
        tetris.onSent(message10).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(1L, globalSafe);

        var gz = redis.zrange(List.of("tetris:minids", "0", "-1")).await().indefinitely();
        assertNotNull(gz);
    }

    @Test
    public void testAckAdvancesNextExpectedId() {
        Message message3 = msg(3L, recipient1);
        Message message4 = msg(4L, recipient1);
        Message message6 = msg(6L, recipient1);

        tetris.onSent(message3).await().indefinitely();
        tetris.onSent(message4).await().indefinitely();
        tetris.onSent(message6).await().indefinitely();

        tetris.onAck(message6).await().indefinitely();
        assertEquals(3L, tetris.computeGlobalSafeDeleteId().await().indefinitely());

        tetris.onAck(message3).await().indefinitely();
        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testAckOutOfOrder() {
        Message message1 = msg(1L, recipient1);
        Message message2 = msg(2L, recipient1);
        Message message3 = msg(3L, recipient1);

        tetris.onSent(message1).await().indefinitely();
        tetris.onSent(message2).await().indefinitely();
        tetris.onSent(message3).await().indefinitely();

        tetris.onAck(message3).await().indefinitely();
        assertEquals(1L, tetris.computeGlobalSafeDeleteId().await().indefinitely());

        tetris.onAck(message1).await().indefinitely();
        assertEquals(2L, tetris.computeGlobalSafeDeleteId().await().indefinitely());

        tetris.onAck(message2).await().indefinitely();
        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testDuplicateAckIsSafe() {
        Message message1 = msg(1L, recipient1);
        Message message2 = msg(2L, recipient1);
        tetris.onSent(message1).await().indefinitely();
        tetris.onSent(message2).await().indefinitely();

        tetris.onAck(message1).await().indefinitely();
        tetris.onAck(message1).await().indefinitely();

        // after acking 1, min unacked == 2 => global safe for single-client scenario becomes 2,
        // and with no other clients present computeGlobalSafeDeleteId() should return 2
        assertEquals(2L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testGapAckDoesNotAdvanceSafeDelete() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();

        // ack middle one only
        tetris.onAck(msg(2L, recipient1)).await().indefinitely();

        // earliest unacked is still 1 -> safe remains 1
        assertEquals(1L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testLateAcksCatchUp() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();

        tetris.onAck(msg(3L, recipient1)).await().indefinitely();
        assertEquals(1L, tetris.computeGlobalSafeDeleteId().await().indefinitely());

        tetris.onAck(msg(1L, recipient1)).await().indefinitely();
        tetris.onAck(msg(2L, recipient1)).await().indefinitely();

        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testAckBeforeAnySentIsIgnored() {
        tetris.onAck(msg(5L, recipient1)).await().indefinitely();
        assertEquals(0L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testDisconnectAdvancesSafeDelete() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();

        tetris.onDisconnect(recipient1).await().indefinitely();

        // onDisconnect should set safe to highestSentId (3)
        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testDisconnectBeforeAnySentDoesNothing() {
        tetris.onDisconnect(recipient1).await().indefinitely();
        assertEquals(0L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testDisconnectHandlesGaps() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();
        tetris.onSent(msg(4L, recipient1)).await().indefinitely();

        tetris.onAck(msg(1L, recipient1)).await().indefinitely();

        tetris.onDisconnect(recipient1).await().indefinitely();

        // highestSentId = 4 -> safe = 4
        assertEquals(5L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testAckExactlyAtDisconnectCutoff() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();

        tetris.onDisconnect(recipient1).await().indefinitely(); // safe = 2

        tetris.onSent(msg(3L, recipient1)).await().indefinitely();
        tetris.onAck(msg(3L, recipient1)).await().indefinitely();

        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testDisconnectCleansAckZset() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();

        tetris.onDisconnect(recipient1).await().indefinitely();
        String ackKey = "tetris:re:" + recipient1 + ":ack";

        String count = redis.zcount(ackKey, "-inf", "+inf")
                .await().indefinitely()
                .toString();

        // after disconnect we keep only the max (2) -> zset size == 1
        assertEquals("0", count);
        assertEquals(3L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testAckAfterDisconnectWorksNormally() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();

        tetris.onDisconnect(recipient1).await().indefinitely(); // safe = 2

        tetris.onSent(msg(3L, recipient1)).await().indefinitely();
        tetris.onAck(msg(3L, recipient1)).await().indefinitely();

        assertEquals(4L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testMultipleRecipients() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(10L, recipient2)).await().indefinitely();

        tetris.onAck(msg(1L, recipient1)).await().indefinitely();
        tetris.onAck(msg(10L, recipient2)).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(2L, globalSafe);
    }

    @Test
    public void testOneRecipientBlocksGlobal() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(10L, recipient2)).await().indefinitely();
        tetris.onSent(msg(11L, recipient2)).await().indefinitely();

        tetris.onAck(msg(10L, recipient2)).await().indefinitely();
        tetris.onAck(msg(11L, recipient2)).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(1L, globalSafe);
    }

    @Test
    public void testScanMultiIteration() {
        for (int i = 0; i < 150; i++) {
            String r = UUID.randomUUID().toString();
            tetris.onSent(msg(5L, r)).await().indefinitely();
            tetris.onAck(msg(5L, r)).await().indefinitely();
        }

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(6L, globalSafe);
    }

    @Test
    public void testMultipleClientsOneEmptyOneNot() {
        // recipient1 fully acked
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onAck(msg(1L, recipient1)).await().indefinitely();
        tetris.onAck(msg(2L, recipient1)).await().indefinitely();

        // recipient2 has unacked messages
        tetris.onSent(msg(10L, recipient2)).await().indefinitely();
        tetris.onSent(msg(12L, recipient2)).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(10L, globalSafe);

        var z1 = redis.zrange(List.of("tetris:re:" + recipient1 + ":ack", "0", "-1")).await().indefinitely();
        assertTrue(z1.size()==0, "recipient1 acks should be empty");

        var z2 = redis.zrange(List.of("tetris:re:" + recipient2 + ":ack", "0", "-1")).await().indefinitely();
        assertEquals(List.of("10", "12").toString(), z2.toString(), "recipient2 should have unacked messages");
    }

    @Test
    public void testZeroIdsIgnored() {
        tetris.onSent(msg(0L, recipient1)).await().indefinitely();
        tetris.onSent(msg(5L, recipient2)).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(5L, globalSafe);
    }

    @Test
    public void testGlobalSafeBecomesMinusOneIfAllZeroOrEmpty() {
        tetris.onSent(msg(0L, recipient1)).await().indefinitely();
        tetris.onAck(msg(0L, recipient1)).await().indefinitely();

        tetris.onSent(msg(0L, recipient2)).await().indefinitely();
        tetris.onAck(msg(0L, recipient2)).await().indefinitely();

        Long globalSafe = tetris.computeGlobalSafeDeleteId().await().indefinitely();
        assertEquals(0L, globalSafe);
    }

    @Test
    public void testFindHeavyClientsNone() {
        // no clients -> none heavy
        var heavy = tetris.findHeavyClients(2, 10).await().indefinitely();
        assertTrue(heavy.isEmpty(), "No heavy clients expected");
    }

    @Test
    public void testFindHeavyClientsAllBelowThreshold() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(1L, recipient2)).await().indefinitely();

        var heavy = tetris.findHeavyClients(3, 10).await().indefinitely();
        assertTrue(heavy.isEmpty(), "No client exceeds threshold 3");
    }

    @Test
    public void testFindHeavyClientsSomeAboveThreshold() {
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();

        tetris.onSent(msg(1L, recipient2)).await().indefinitely();

        var heavy = tetris.findHeavyClients(2, 10).await().indefinitely();
        assertEquals(1, heavy.size());
        assertTrue(heavy.contains(recipient1));
        assertFalse(heavy.contains(recipient2));
    }

    @Test
    public void testFindHeavyClientsRespectsLimit() {
        for (int i = 0; i < 5; i++) {
            String r = UUID.randomUUID().toString();
            tetris.onSent(msg(1L, r)).await().indefinitely();
            tetris.onSent(msg(2L, r)).await().indefinitely();
            tetris.onSent(msg(3L, r)).await().indefinitely();
        }

        var heavy = tetris.findHeavyClients(2, 3).await().indefinitely();
        assertEquals(3, heavy.size(), "Should respect limit");
    }

    @Test
    public void testFindHeavyClientsIgnoresAcked() {
        // recipient1 = 3 unacked initially, then ack 2
        tetris.onSent(msg(1L, recipient1)).await().indefinitely();
        tetris.onSent(msg(2L, recipient1)).await().indefinitely();
        tetris.onSent(msg(3L, recipient1)).await().indefinitely();

        tetris.onAck(msg(2L, recipient1)).await().indefinitely();

        var heavy = tetris.findHeavyClients(2, 10).await().indefinitely();
        assertEquals(1, heavy.size());
        assertTrue(heavy.contains(recipient1));
    }

    @Test
    public void testFindHeavyClientsWithZeroIds() {
        tetris.onSent(msg(0L, recipient1)).await().indefinitely();
        tetris.onSent(msg(1L, recipient2)).await().indefinitely();
        tetris.onSent(msg(2L, recipient2)).await().indefinitely();
        tetris.onSent(msg(3L, recipient2)).await().indefinitely();

        var heavy = tetris.findHeavyClients(2, 10).await().indefinitely();
        assertEquals(1, heavy.size());
        assertTrue(heavy.contains(recipient2));
        assertFalse(heavy.contains(recipient1));
    }

    @Test
    public void testPrimedFlagAndRehydrateFromOutbox() {
        // fresh Redis (flushed in setup) → state is not primed
        assertFalse(tetris.isPrimed().await().indefinitely());

        // rebuild pending state from the durable outbox (recipient → ids)
        tetris.rehydrate(java.util.Map.of(
                recipient1, List.of(5L, 6L),
                recipient2, List.of(9L))).await().indefinitely();

        // now primed, and the safe-delete id is the global min pending (5)
        assertTrue(tetris.isPrimed().await().indefinitely());
        assertEquals(5L, tetris.computeGlobalSafeDeleteId().await().indefinitely());

        // an ack on the lowest id advances the boundary to the next pending (6)
        tetris.onAck(msg(5L, recipient1)).await().indefinitely();
        assertEquals(6L, tetris.computeGlobalSafeDeleteId().await().indefinitely());
    }

    @Test
    public void testNotPrimedAfterStateLoss() {
        tetris.rehydrate(java.util.Map.of(recipient1, List.of(1L))).await().indefinitely();
        assertTrue(tetris.isPrimed().await().indefinitely());
        redis.flushall(List.of()).await().indefinitely();
        assertFalse(tetris.isPrimed().await().indefinitely(), "flushed Redis must read as unprimed");
    }

    // helper to build Message objects used by tests
    private Message msg(long id, String recipientId) {
        return Message.builder()
                .id(id)
                .ackId(id)
                .recipientId(recipientId)
                .senderId(recipientId)
                .build();
    }
}
