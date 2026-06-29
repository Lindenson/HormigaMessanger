package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.ChatQuery;
import org.hormigas.ws.domain.conversation.ChatStats;
import org.hormigas.ws.domain.conversation.Conversation;

import java.util.List;

/**
 * Driving (port-IN) use case for platform operators (ADMIN). Unlike {@link Chats}, these reads are
 * <b>not membership-guarded and not per-side filtered</b> — an admin sees every chat regardless of
 * who deleted (hid) or blocked it. The REST adapter restricts the caller to ADMIN; this interface
 * carries no auth itself. Implemented in core; the adapter depends on this abstraction, not the impl.
 */
public interface AdminChats {

    /** All chats matching the {@link ChatQuery} (filters/sort/page), unfiltered by participant state. */
    Uni<List<Conversation>> list(ChatQuery query);

    /** Total chats matching the query's filters (ignoring paging) — for pagination totals. */
    Uni<Long> count(ChatQuery query);

    /** Platform-wide counts for the admin dashboard. */
    Uni<ChatStats> stats();
}
