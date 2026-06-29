package org.hormigas.ws.infrastructure.cache.conversation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationDirectory;
import org.hormigas.ws.ports.conversation.ConversationManager;

import java.time.Duration;

/**
 * In-process L1 cache (Caffeine) for the hot conversation read (Phase 2, 4.1). The send-guard looks up
 * membership/block on every inbound message; without this each message was a Postgres round-trip
 * (load findings: the send path is otherwise DB-bound). On a miss we load via {@link ConversationManager}
 * and cache the row; writes (block/unblock/soft-delete) {@link #invalidate} it (write-through). A short
 * {@code expireAfterWrite} TTL bounds staleness even without an explicit invalidation.
 *
 * <p>Single-instance correct. For multiple instances / sharding we will add a distributed L2 (Redis)
 * behind {@link ConversationDirectory} plus cross-instance invalidation (Redis pub/sub) so a block on
 * one node evicts the entry on all nodes — deferred until the service runs as more than one instance.
 */
@ApplicationScoped
public class CachedConversationDirectory implements ConversationDirectory {

    @Inject
    ConversationManager manager;

    @ConfigProperty(name = "processing.messages.conversation-cache.max-size", defaultValue = "100000")
    long maxSize;

    @ConfigProperty(name = "processing.messages.conversation-cache.ttl-seconds", defaultValue = "60")
    int ttlSeconds;

    private Cache<String, Conversation> cache;

    @PostConstruct
    void init() {
        cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .build();
    }

    @Override
    public Uni<Conversation> findById(String id) {
        Conversation hit = cache.getIfPresent(id);
        if (hit != null) {
            return Uni.createFrom().item(hit);
        }
        // Miss → load and populate. Absent conversations are NOT negatively cached (an absent row is
        // rare and may be created imminently; the next lookup loads it fresh).
        return manager.findById(id).invoke(loaded -> {
            if (loaded != null) {
                cache.put(id, loaded);
            }
        });
    }

    @Override
    public void invalidate(String id) {
        cache.invalidate(id);
    }
}
