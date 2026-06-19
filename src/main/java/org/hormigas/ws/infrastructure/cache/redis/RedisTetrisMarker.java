package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.tetris.TetrisMarker;

import java.util.ArrayList;
import java.util.List;


/**
 * Redis-based implementation of TetrisMarker providing message ordering,
 * per-recipient tracking, and global safe-delete computation.
 *
 * <p>This component maintains several Redis structures:</p>
 * <ul>
 *     <li><b>Per-recipient ZSET</b> (tetris:re:&lt;id&gt;:ack) — tracks unacknowledged message IDs.</li>
 *     <li><b>Global min ZSET</b> (tetris:minids) — stores the smallest pending ID for each client.</li>
 *     <li><b>Counts hash</b> (tetris:re:cnt) — tracks the number of pending messages per client.</li>
 *     <li><b>LAST_ID_KEY</b> (tetris:lastid) — remembers the last seen message ID globally.</li>
 * </ul>
 *
 * <p>The global safe-delete ID is computed as follows:</p>
 * <ol>
 *     <li>If <code>tetris:minids</code> contains at least one non-zero score,
 *         the smallest one is returned.</li>
 *     <li>Otherwise, the system falls back to <code>LAST_ID_KEY</code>:
 *         returning <code>(lastId + 1)</code>
 *         or <code>0</code> if no messages were ever seen.</li>
 * </ol>
 *
 * <p>This ensures that the global safe-delete pointer always moves forward,
 * even when all recipients are fully acknowledged.</p>
 */
@Slf4j
@Startup
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class RedisTetrisMarker implements TetrisMarker<Message> {

    private static final String RECIPIENT_KEY_PREFIX = "tetris:re:";
    private static final String ACKS_SUFFIX = ":ack";
    private static final String GLOBAL_MIN_KEY = "tetris:minids";
    private static final String COUNTS_KEY = "tetris:re:cnt";
    private static final String LAST_ID_KEY = "tetris:lastid";
    private static final String PRIMED_KEY = "tetris:primed";

    @Inject
    RedisAPI redis;


    /**
     * ---------------- SCRIPTS PRELOADED IN JAVA -------------------
     */

    private static String LUA_ON_SENT_SHA;
    private static final String LUA_ON_SENT = """
            local per = KEYS[1]
            local gmin = KEYS[2]
            local counts = KEYS[3]
            local lastk = KEYS[4]
            local msg = tonumber(ARGV[1])
            local client = ARGV[2]

            -- ZADD (no flags) returns the count of NEWLY added members; onSent runs more than once
            -- per message (inbound persist + every outbox re-delivery), so only count a genuinely
            -- new pending id — otherwise HINCRBY drifts the per-client counter upward.
            local added = redis.call('ZADD', per, msg, tostring(msg))
            if added == 1 then
                redis.call('HINCRBY', counts, client, 1)
            end
            redis.call('SET', lastk, tostring(msg))
            
            local minMember = redis.call('ZRANGE', per, 0, 0)
            if minMember[1] then
                local safe = tonumber(minMember[1])
                if safe ~= 0 then
                    redis.call('ZADD', gmin, safe, client)
                else
                    redis.call('ZREM', gmin, client)
                end
            else
                redis.call('ZREM', gmin, client)
            end
            return 1
            """;

    private static String LUA_ON_ACK_SHA;
    private static final String LUA_ON_ACK = """
            local per = KEYS[1]
            local gmin = KEYS[2]
            local counts = KEYS[3]
            local msg = tostring(ARGV[1])
            local client = ARGV[2]

            -- Only decrement the count when an id was actually pending (mirror onSent's ZADD-guard),
            -- otherwise a duplicate/late ACK drifts the count below the true ZCARD.
            local removed = redis.call('ZREM', per, msg)
            if removed == 1 then
                local cnt = redis.call('HINCRBY', counts, client, -1)
                if tonumber(cnt) < 0 then
                    redis.call('HSET', counts, client, 0)
                end
            end

            local minMember = redis.call('ZRANGE', per, 0, 0)
            if minMember[1] then
                local safe = tonumber(minMember[1])
                if safe ~= 0 then
                    redis.call('ZADD', gmin, safe, client)
                else
                    redis.call('ZREM', gmin, client)
                end
            else
                redis.call('ZREM', gmin, client)
            end
            return 1
            """;

    private static String LUA_ON_DISCONNECT_SHA;
    private static final String LUA_ON_DISCONNECT = """
            local per = KEYS[1]
            local gmin = KEYS[2]
            local counts = KEYS[3]
            local client = ARGV[1]

            redis.call('DEL', per)
            -- remove the count field entirely (HSET 0 leaked a field per distinct client forever)
            redis.call('HDEL', counts, client)
            redis.call('ZREM', gmin, client)
            return 1
            """;

    private static String LUA_COMPUTE_GLOBAL_SHA;
    private static final String LUA_COMPUTE_GLOBAL = """
            local gmin = KEYS[1]
            local lastk = KEYS[2]
            
            local vals = redis.call('ZRANGE', gmin, 0, 0, 'WITHSCORES')
            if vals and #vals > 0 then
                local score = tonumber(vals[2])
                if score and score ~= 0 then
                    return tostring(score)
                end
            end
            
            local last = redis.call('GET', lastk)
            if not last or last == false then return '0' end
            return tostring((tonumber(last) or -1) + 1)
            """;

    /**
     * ---------------------------------------------------------------
     */


    @PostConstruct
    void init() {
        try {
            // Clean all per-recipient keys. SCAN (cursor) instead of KEYS so a large keyspace is
            // never blocked, and UNLINK in one batch per page instead of a blocking DEL per key.
            String cursor = "0";
            do {
                io.vertx.mutiny.redis.client.Response page = redis.scan(List.of(
                        cursor, "MATCH", RECIPIENT_KEY_PREFIX + "*", "COUNT", "500"))
                        .await().indefinitely();
                cursor = page.get(0).toString();
                io.vertx.mutiny.redis.client.Response keys = page.get(1);
                if (keys != null && keys.size() > 0) {
                    List<String> batch = new ArrayList<>(keys.size());
                    keys.forEach(k -> batch.add(k.toString()));
                    redis.unlink(batch).await().indefinitely();
                }
            } while (!"0".equals(cursor));

            redis.unlink(List.of(GLOBAL_MIN_KEY, COUNTS_KEY, LAST_ID_KEY))
                    .await().indefinitely();

            log.info("Redis Tetris initial cleanup complete");

            final String load = "LOAD";
            LUA_ON_SENT_SHA = redis.script(List.of(load, LUA_ON_SENT)).await().indefinitely().toString();
            LUA_ON_ACK_SHA = redis.script(List.of(load, LUA_ON_ACK)).await().indefinitely().toString();
            LUA_ON_DISCONNECT_SHA = redis.script(List.of(load, LUA_ON_DISCONNECT)).await().indefinitely().toString();
            LUA_COMPUTE_GLOBAL_SHA = redis.script(List.of(load, LUA_COMPUTE_GLOBAL)).await().indefinitely().toString();

        } catch (Exception e) {
            log.error("Redis cleanup failed", e);
        }
    }


    /**
     * ---------------- LOW-LEVEL SAFE CALLER -------------------
     */

    private Uni<?> eval(String script, List<String> keys, List<String> args) {
        List<String> full = new ArrayList<>();
        full.add(script);
        full.add(String.valueOf(keys.size()));
        full.addAll(keys);
        full.addAll(args);
        return redis.eval(full);
    }

    private Uni<?> evalSha(String script, List<String> keys, List<String> args) {
        List<String> full = new ArrayList<>();
        full.add(script);
        full.add(String.valueOf(keys.size()));
        full.addAll(keys);
        full.addAll(args);
        return redis.evalsha(full);
    }

    /**
     * Run a script by its cached SHA, falling back to a full EVAL (which re-caches it) ONLY when
     * Redis reports NOSCRIPT — e.g. after a Redis restart/FLUSH. The fallback is gated to NOSCRIPT
     * so a non-idempotent script (onSent's HINCRBY/SET) is never re-run on a transient error.
     */
    @SuppressWarnings("unchecked")
    private Uni<?> evalCached(String sha, String rawScript, List<String> keys, List<String> args) {
        // If the SHA never loaded (Redis was down at @Startup), run the full script directly — EVAL
        // executes and re-caches it, so ops recover without a JVM restart.
        if (sha == null || sha.isBlank()) {
            return eval(rawScript, keys, args);
        }
        return ((Uni<Object>) evalSha(sha, keys, args))
                .onFailure(RedisTetrisMarker::isNoScript)
                .recoverWithUni(() -> {
                    log.warn("Tetris script not cached in Redis (NOSCRIPT) — reloading via EVAL");
                    return (Uni<Object>) eval(rawScript, keys, args);
                });
    }

    private static boolean isNoScript(Throwable t) {
        return t != null && t.getMessage() != null && t.getMessage().contains("NOSCRIPT");
    }


    /**
     * ---------------- OPERATIONS -------------------
     */


    /**
     * Records a newly sent message:
     *
     * <ul>
     *     <li>Adds the message ID to the recipient's per-recipient ZSET.</li>
     *     <li>Increments the pending message counter for the client.</li>
     *     <li>Updates <code>LAST_ID_KEY</code> with the latest global message ID.</li>
     *     <li>Updates the global minimum ZSET so the safe-delete logic can find the
     *         smallest pending ID across all recipients.</li>
     * </ul>
     *
     * <p>If anything fails, the stage is marked as failed but no exception is propagated.</p>
     */
    @Override
    public Uni<StageResult<Message>> onSent(@NotNull Message message) {
        String rid = message.getRecipientId();
        long id = message.getId();
        if (rid == null || id <= 0) return Uni.createFrom().item(StageResult.failed());

        return evalCached(
                LUA_ON_SENT_SHA, LUA_ON_SENT,
                List.of(recipientKey(rid) + ACKS_SUFFIX, GLOBAL_MIN_KEY, COUNTS_KEY, LAST_ID_KEY),
                List.of(String.valueOf(id), rid)
        )
                .replaceWith(StageResult.<Message>passed())
                .onFailure().invoke(() -> log.error("Redis Tetris onSent failed"))
                .onFailure().recoverWithItem(StageResult.failed());
    }


    /**
     * Processes an acknowledgment:
     *
     * <ul>
     *     <li>Removes the acknowledged message ID from the recipient ZSET.</li>
     *     <li>Decrements the per-client pending counter, never going below zero.</li>
     *     <li>Recomputes the client's minimum pending message ID and updates
     *         the global minimum ZSET accordingly.</li>
     * </ul>
     *
     * <p>Failures are fully recovered and produce a failed StageResult.</p>
     */
    @Override
    public Uni<StageResult<Message>> onAck(@NotNull Message message) {
        String rid = message.getSenderId();
        long id = message.getAckId();
        if (rid == null || id <= 0) return Uni.createFrom().item(StageResult.failed());

        return evalCached(
                LUA_ON_ACK_SHA, LUA_ON_ACK,
                List.of(recipientKey(rid) + ACKS_SUFFIX, GLOBAL_MIN_KEY, COUNTS_KEY),
                List.of(String.valueOf(id), rid)
        )
                .replaceWith(StageResult.<Message>passed())
                .onFailure().invoke(() -> log.error("Redis Tetris onAck failed"))
                .onFailure().recoverWithItem(StageResult.failed());
    }


    /**
     * Handles client disconnection:
     *
     * <ul>
     *     <li>Deletes the recipient's pending-message ZSET.</li>
     *     <li>Resets the client's pending-message counter to zero.</li>
     *     <li>Removes the client from the global minimum ZSET.</li>
     * </ul>
     *
     * <p>This ensures that stale clients do not affect the global safe-delete calculation.</p>
     */
    @Override
    public Uni<StageResult<Message>> onDisconnect(@NotNull String rid) {
        if (rid == null) return Uni.createFrom().item(StageResult.failed());

        return evalCached(
                LUA_ON_DISCONNECT_SHA, LUA_ON_DISCONNECT,
                List.of(recipientKey(rid) + ACKS_SUFFIX, GLOBAL_MIN_KEY, COUNTS_KEY),
                List.of(rid)
        )
                .replaceWith(StageResult.<Message>passed())
                .onFailure().invoke(() -> log.error("Redis Tetris onDisconnect failed"))
                .onFailure().recoverWithItem(StageResult.failed());
    }


    /**
     * Computes the globally safe message ID that can be deleted.
     *
     * <p>The logic is implemented in the embedded Lua script and works as follows:</p>
     *
     * <ol>
     *     <li>Lookup the smallest non-zero score in the global min ZSET
     *         (<code>tetris:minids</code>).
     *         If found, it represents the lowest unacknowledged message across all clients,
     *         so it is returned as the safe-delete boundary.</li>
     *
     *     <li>If the ZSET is empty or the only value is <code>0</code>,
     *         the system falls back to <code>LAST_ID_KEY</code> (<code>tetris:lastid</code>).
     *         This key holds the last observed message ID globally, updated by <code>onSent</code>.</li>
     *
     *     <li>If no value exists in <code>LAST_ID_KEY</code>,
     *         the method returns <code>0</code>.</li>
     *
     *     <li>Otherwise, the method returns <code>(lastId + 1)</code>,
     *         meaning: “all messages up to and including lastId are safe to delete”.</li>
     * </ol>
     *
     * <p>This mechanism ensures forward progress of the global cleanup pointer,
     * even when all recipients have acknowledged all messages.</p>
     *
     * @return the global safe-delete message ID, or 0 if unknown.
     */
    @Override
    public Uni<Long> computeGlobalSafeDeleteId() {
        return evalCached(
                LUA_COMPUTE_GLOBAL_SHA, LUA_COMPUTE_GLOBAL,
                List.of(GLOBAL_MIN_KEY, LAST_ID_KEY),
                List.of()
        ).map(resp -> {
                    try {
                        return Long.parseLong(resp.toString());
                    } catch (Exception e) {
                        return 0L;
                    }
                })
                .onFailure().invoke(() -> log.error("Redis Tetris computeGlobalSafeDeleteId failed"))
                .onFailure().recoverWithItem(0L);
    }


    /**
     * Returns a list of clients whose pending-message counters are above
     * (or equal to) the specified threshold, up to the specified limit.
     *
     * <p>The method reads the <code>tetris:re:cnt</code> hash and filters clients
     * based on their current queue size.</p>
     *
     * @param threshold minimum count to be considered "heavy"
     * @param limit maximum number of clients to return
     * @return the list of heavy client IDs
     */
    @Override
    public Uni<List<String>> findHeavyClients(int threshold, int limit) {
        try {
            return redis.hgetall(COUNTS_KEY)
                    .map(resp -> {
                        List<String> heavy = new ArrayList<>();
                        if (resp != null && !resp.getKeys().isEmpty()) {
                            for (String key : resp.getKeys()) {
                                long cnt = Long.parseLong(resp.get(key).toString());
                                if (cnt >= threshold) {
                                    heavy.add(key);
                                    if (heavy.size() >= limit)
                                        break;
                                }
                            }
                        }
                        return heavy;
                    });
        } catch (Exception e) {
            log.error("Redis cleanup failed", e);
            return Uni.createFrom().item(List.of());
        }
    }


    @Override
    public Uni<Boolean> isPrimed() {
        return redis.exists(List.of(PRIMED_KEY))
                .map(r -> r != null && r.toInteger() == 1)
                .onFailure().recoverWithItem(false);
    }

    @Override
    public Uni<Void> rehydrate(java.util.Map<String, List<Long>> pendingByRecipient) {
        // isPrimed() == false implies the state was wiped (cold start / flush), so the keys below
        // are empty — we rebuild them with absolute writes (no pre-delete, no races between keys).
        List<Uni<io.vertx.mutiny.redis.client.Response>> ops = new ArrayList<>();
        long maxAll = -1;
        for (var e : pendingByRecipient.entrySet()) {
            String rid = e.getKey();
            List<Long> ids = e.getValue();
            if (rid == null || ids == null || ids.isEmpty()) continue;

            List<String> zadd = new ArrayList<>();
            zadd.add(recipientKey(rid) + ACKS_SUFFIX);
            long min = Long.MAX_VALUE;
            for (Long id : ids) {
                if (id == null) continue;
                zadd.add(String.valueOf(id)); // score
                zadd.add(String.valueOf(id)); // member
                if (id < min) min = id;
                if (id > maxAll) maxAll = id;
            }
            if (zadd.size() <= 1) continue;
            ops.add(redis.zadd(zadd));
            ops.add(redis.hset(List.of(COUNTS_KEY, rid, String.valueOf(ids.size()))));
            if (min != 0 && min != Long.MAX_VALUE) {
                ops.add(redis.zadd(List.of(GLOBAL_MIN_KEY, String.valueOf(min), rid)));
            }
        }
        if (maxAll >= 0) ops.add(redis.set(List.of(LAST_ID_KEY, String.valueOf(maxAll))));
        // primed last — only meaningful once the pending state above is in place
        ops.add(redis.set(List.of(PRIMED_KEY, "1")));

        log.info("Rehydrating Tetris from outbox: {} recipients, lastId={}", pendingByRecipient.size(), maxAll);
        return Uni.join().all(ops).andCollectFailures().replaceWithVoid()
                .onFailure().invoke(err -> log.error("Tetris rehydrate failed", err))
                .onFailure().recoverWithUni(Uni.createFrom().voidItem());
    }

    private String recipientKey(String id) {
        return RECIPIENT_KEY_PREFIX + id;
    }
}
