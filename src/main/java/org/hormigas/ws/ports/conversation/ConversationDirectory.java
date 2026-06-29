package org.hormigas.ws.ports.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.Conversation;

/**
 * Driven port for the <b>hot</b> conversation read (membership + block state), looked up on every
 * inbound CHAT_IN / SIGNAL_IN by the send-guard. It fronts {@link ConversationManager} with a cache so
 * the guard does not hit the database per message.
 *
 * <p><b>Current implementation:</b> an in-process L1 cache (Caffeine) with write-through invalidation
 * on block/unblock/soft-delete plus a short TTL backstop — correct for a single instance.
 *
 * <p><b>Future (multi-instance + sharding):</b> add a distributed L2 (Redis) behind this same port and
 * cross-instance invalidation (Redis pub/sub), so a block/unblock on one node evicts the entry on all
 * nodes. Deferred until the service runs as more than one instance; the port seam makes it a drop-in.
 */
public interface ConversationDirectory {

    /** The conversation by id (cached), or {@code null} if none exists. */
    Uni<Conversation> findById(String id);

    /** Evict a conversation from the cache (call after a block/unblock/soft-delete write). */
    void invalidate(String id);
}
