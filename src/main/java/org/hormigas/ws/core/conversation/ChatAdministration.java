package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.conversation.ChatQuery;
import org.hormigas.ws.domain.conversation.ChatSort;
import org.hormigas.ws.domain.conversation.ChatStats;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationManager;

import java.util.List;

/**
 * Admin chat read use cases (scope B): platform-wide listing with filters/sort/paging, totals and
 * dashboard stats. Reads via the {@link ConversationManager} driven port; applies only the paging
 * policy (clamp the page size) — no membership/visibility filtering, since an admin sees everything.
 */
@ApplicationScoped
public class ChatAdministration implements AdminChats {

    /** Default admin page size when the caller doesn't specify one. */
    public static final int DEFAULT_LIMIT = 50;
    /** Hard cap so an admin page request cannot be unbounded. */
    public static final int MAX_LIMIT = 200;

    @Inject
    ConversationManager manager;

    @Override
    public Uni<List<Conversation>> list(ChatQuery query) {
        return manager.findAll(normalize(query));
    }

    @Override
    public Uni<Long> count(ChatQuery query) {
        return manager.count(normalize(query));
    }

    @Override
    public Uni<ChatStats> stats() {
        return manager.stats();
    }

    /** Clamp paging and default the sort, so the adapter always receives a sane query. */
    private ChatQuery normalize(ChatQuery q) {
        int limit = q.limit() <= 0 ? DEFAULT_LIMIT : Math.min(q.limit(), MAX_LIMIT);
        int offset = Math.max(0, q.offset());
        ChatSort sort = q.sort() == null ? ChatSort.UPDATED_DESC : q.sort();
        return new ChatQuery(q.participantId(), q.conversationId(), q.blockedOnly(),
                q.createdFrom(), q.createdTo(), sort, limit, offset);
    }
}
