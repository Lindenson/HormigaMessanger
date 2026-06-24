package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.ports.deadletter.DeliveryConfirmations;

import java.util.List;

/**
 * Redis-set-backed delivery confirmations for the dead-letter retract sweep (ADR-014). SYSTEM_ACK
 * {@code SADD}s the messageId; the cleanup sweep peeks a batch, deletes the matching drafts, then
 * clears (SREM) — so a confirmation lives until its draft is actually removed (no TTL/tick race).
 */
@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class RedisDeliveryConfirmations implements DeliveryConfirmations {

    private static final String KEY = "deadletter:confirmed";

    @Inject
    ReactiveRedisDataSource redis;

    private ReactiveSetCommands<String, String> set;

    @PostConstruct
    void init() {
        set = redis.set(String.class);
    }

    @Override
    public Uni<Void> confirm(String messageId) {
        if (messageId == null || messageId.isBlank()) return Uni.createFrom().voidItem();
        return set.sadd(KEY, messageId).replaceWithVoid()
                .onFailure().invoke(e -> log.error("confirm({}) failed: {}", messageId, e.getMessage()))
                .onFailure().recoverWithItem((Void) null);
    }

    @Override
    public Uni<List<String>> peek(int limit) {
        return set.smembers(KEY)
                .map(members -> members.stream().limit(limit).toList())
                .onFailure().recoverWithItem(List.of());
    }

    @Override
    public Uni<Void> clear(List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) return Uni.createFrom().voidItem();
        return set.srem(KEY, messageIds.toArray(new String[0])).replaceWithVoid()
                .onFailure().recoverWithItem((Void) null);
    }
}
