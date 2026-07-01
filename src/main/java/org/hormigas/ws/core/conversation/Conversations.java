package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.conversation.CreateResult;
import org.hormigas.ws.domain.conversation.Guarded;
import org.hormigas.ws.domain.conversation.Outcome;
import org.hormigas.ws.domain.conversation.SendCheck;
import org.hormigas.ws.domain.conversation.SendDecision;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.conversation.ConversationDirectory;
import org.hormigas.ws.ports.conversation.ConversationManager;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.ports.message.MessageModeration;
import org.hormigas.ws.ports.message.ReadReceipts;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The conversation use cases (core): create/query chats and the membership-guarded operations on a
 * chat (history read, moderation, read receipts). Trigger-agnostic — invoked by the REST adapter and
 * the Order-event adapter over these same operations. Authorization (membership/block) lives here, in
 * the shared {@link #guardedRead} guard — never in an adapter.
 */
@ApplicationScoped
public class Conversations implements Chats {

    @Inject
    ConversationManager repository;

    /** Cached read-side for the hot membership/block lookup (Phase 2, 4.1). Reads go through here; */
    @Inject
    ConversationDirectory directory;

    @Inject
    IdGenerator idGenerator;

    @Inject
    History<Message> history;

    @Inject
    MessageModeration moderation;

    @Inject
    ReadReceipts receipts;

    @Inject
    org.hormigas.ws.core.router.persist.ReadStatusBatcher readStatus;

    /** Default history page size when the caller doesn't specify one. */
    public static final int DEFAULT_HISTORY_LIMIT = 200;
    /** Hard cap so a caller cannot request an unbounded page. */
    public static final int MAX_HISTORY_LIMIT = 500;

    /** Idempotent create: returns the existing chat for the pair, or a new one. */
    public Uni<CreateResult> createChat(String clientId, String masterId, Map<String, String> metadata) {
        return repository.findByPair(clientId, masterId)
                .flatMap(existing -> {
                    if (existing != null) {
                        return Uni.createFrom().item(new CreateResult(existing, false));
                    }
                    Instant now = Instant.now();
                    Conversation fresh = new Conversation(
                            idGenerator.generateId(), clientId, masterId,
                            metadata != null ? metadata : Map.of(), false, false, now, now);
                    return repository.insertIfAbsent(fresh)
                            .map(saved -> new CreateResult(saved, true));
                });
    }

    public Uni<List<Conversation>> listChats(String participantId) {
        return repository.findByParticipant(participantId);
    }

    public Uni<Conversation> findById(String id) {
        return directory.findById(id);
    }

    /**
     * Resolve the conversation for a participant pair. Used by event adapters that key by pair
     * (e.g. freeze-on-contract), since cross-service events carry {@code (clientId, masterId)},
     * not the messenger's internal conversation id.
     */
    public Uni<Conversation> findByPair(String clientId, String masterId) {
        return repository.findByPair(clientId, masterId);
    }

    /**
     * Delete the chat for the caller ("delete for me"): sets the caller's delete watermark to the
     * chat's latest messageId, so their history/list is floored there until newer messages arrive.
     * Membership-checked; invalidates the cache.
     */
    public Uni<Outcome> hide(String chatId, String userId) {
        return guarded(chatId, userId, () -> repository.hideFor(chatId, userId)
                .invoke(() -> directory.invalidate(chatId)));
    }

    /** Block/unblock the peer for the caller; membership-checked. Invalidates the cache (write-through). */
    public Uni<Outcome> setBlocked(String chatId, String userId, boolean blocked) {
        return guarded(chatId, userId, () -> repository.setBlocked(chatId, userId, blocked)
                .invoke(() -> directory.invalidate(chatId)));
    }

    private Uni<Outcome> guarded(String chatId, String userId, Supplier<Uni<Void>> action) {
        return guardedRead(chatId, userId, conv -> action.get()).map(Guarded::outcome);
    }

    /**
     * Membership-guarded operation over a conversation: resolves the chat, rejects with
     * {@code NOT_FOUND}/{@code FORBIDDEN} when missing/not-a-member, else runs {@code action} and
     * wraps its value as {@code OK}. The single reusable access guard for every conversation-scoped
     * use case — so authorization lives here, never re-implemented in an adapter.
     */
    public <T> Uni<Guarded<T>> guardedRead(String chatId, String userId, Function<Conversation, Uni<T>> action) {
        return directory.findById(chatId).flatMap(c -> {
            if (c == null) return Uni.createFrom().item(Guarded.<T>notFound());
            if (!c.hasParticipant(userId)) return Uni.createFrom().item(Guarded.<T>forbidden());
            return action.apply(c).map(Guarded::ok);
        });
    }

    /**
     * Conversation-scoped, cursor-paginated history read (UC-U50), membership-guarded.
     *
     * <p>Soft-delete (D3, "delete for me"): each participant's read is floored at their own delete
     * watermark — messages at/below it are hidden for that side only (the peer and the admin path are
     * unaffected). The effective cursor is {@code max(since, myWatermark)}, so deletion composes with
     * normal pagination. {@code includeDeleted} bypasses the watermark (a participant's "show deleted"
     * / the admin view) and reads from {@code since} alone. The admin history path does not go through
     * this guard and always reads unfiltered.
     */
    public Uni<Guarded<List<Message>>> history(String chatId, String userId, String since, Integer limit,
                                               boolean includeDeleted) {
        int pageSize = limit == null ? DEFAULT_HISTORY_LIMIT : Math.min(Math.max(1, limit), MAX_HISTORY_LIMIT);
        return guardedRead(chatId, userId, conv -> {
            String floor = includeDeleted ? since : laterCursor(since, conv.deleteCursorFor(userId));
            return history.getByConversation(chatId, floor, pageSize);
        });
    }

    /** The later of two ULID cursors (lexicographic == chronological); null means "no floor". */
    private static String laterCursor(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** Delete a message (only if it exists and is not frozen), membership-guarded (UC-U21). */
    public Uni<Guarded<MessageModeration.DeleteOutcome>> deleteMessage(String chatId, String userId, String messageId) {
        return guardedRead(chatId, userId, conv -> moderation.deleteMessage(chatId, messageId));
    }

    /** Freeze that order's messages, membership-guarded; returns the count frozen (UC-U22). */
    public Uni<Guarded<Integer>> freezeByOrder(String chatId, String userId, String orderId) {
        return guardedRead(chatId, userId, conv -> moderation.freezeByOrder(chatId, orderId));
    }

    /** Mark messages addressed to the caller READ (REST fallback for the READ_IN WS event), guarded. */
    public Uni<Guarded<Integer>> markRead(String chatId, String userId) {
        return guardedRead(chatId, userId, conv -> readStatus.enqueue(chatId, userId));
    }

    /** Per-message status for the chat, membership-guarded. */
    public Uni<Guarded<List<ReadReceipts.Receipt>>> receipts(String chatId, String userId) {
        return guardedRead(chatId, userId, conv -> receipts.receipts(chatId));
    }

    /**
     * Whether {@code senderId} may send into conversation {@code conversationId} right now: the
     * conversation must exist, the sender must be a participant, neither side may have deleted the
     * chat (terminal close), and neither side may have blocked the other (UC-H07 / FR-MSG-01).
     * Enforced on the WS send path.
     */
    public Uni<SendCheck> canSend(String conversationId, String senderId) {
        return evaluateSend(conversationId, senderId).map(SendDecision::check);
    }

    /**
     * Like {@link #canSend} but also returns the resolved {@link Conversation} when the check is
     * {@link SendCheck#ALLOW}, so the caller can derive the authentic recipient (the other participant)
     * instead of trusting a client-supplied {@code recipientId}. The conversation is {@code null} for
     * any non-ALLOW result.
     */
    public Uni<SendDecision> evaluateSend(String conversationId, String senderId) {
        if (conversationId == null || conversationId.isBlank()) {
            return Uni.createFrom().item(new SendDecision(SendCheck.NO_CONVERSATION, null));
        }
        return directory.findById(conversationId).map(c -> {
            if (c == null) return new SendDecision(SendCheck.NO_CONVERSATION, null);
            if (!c.hasParticipant(senderId)) return new SendDecision(SendCheck.NOT_MEMBER, null);
            // Delete is "delete for me" (a per-side history watermark), not a messaging stop — the
            // peer keeps writing and a new message reappears above the watermark. Only block is terminal.
            if (c.isBlocked()) return new SendDecision(SendCheck.BLOCKED, null);
            return new SendDecision(SendCheck.ALLOW, c);
        });
    }

    // Result types (CreateResult, SendDecision, Guarded, Outcome, SendCheck) are declared on the
    // Chats driving port and inherited here — the core never leaks its own types outward.
}
