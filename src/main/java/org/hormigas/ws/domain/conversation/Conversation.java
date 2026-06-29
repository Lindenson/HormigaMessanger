package org.hormigas.ws.domain.conversation;

import java.time.Instant;
import java.util.Map;

/**
 * A 1:1 master↔client chat, keyed by the participant pair (clientId, masterId).
 * Order-agnostic: {@code metadata} is opaque (carries the order reference for the frontend).
 * Framework-free domain record.
 *
 * <p>State is per-participant. Each side can independently <b>block</b> the peer (mutual: disables
 * messaging for the pair) and <b>delete</b> the chat for itself. Delete is a personal "delete for me":
 * each side carries a <b>delete watermark</b> — the messageId (ULID cursor) up to which it deleted its
 * view ({@code deletedFromClient} / {@code deletedFromMaster}, {@code null} = never deleted). History
 * and the chat list are filtered below a side's watermark for that side only; messages above it
 * (e.g. a new order's traffic) reappear naturally. Delete is <b>not</b> terminal for messaging — the
 * peer keeps writing; {@link #isBlocked()} is the only messaging stop. Rows are never removed; the
 * admin path sees everything. {@link #hasParticipant(String)} and {@link #isBlocked()} are the
 * invariants the send-guard and use cases compose.
 */
public record Conversation(
        String id,
        String clientId,
        String masterId,
        Map<String, String> metadata,
        boolean clientBlocked,
        boolean masterBlocked,
        String deletedFromClient,
        String deletedFromMaster,
        Instant createdAt,
        Instant updatedAt
) {
    /** Fresh-chat convenience constructor — neither side has deleted (watermarks null). */
    public Conversation(String id, String clientId, String masterId, Map<String, String> metadata,
                        boolean clientBlocked, boolean masterBlocked, Instant createdAt, Instant updatedAt) {
        this(id, clientId, masterId, metadata, clientBlocked, masterBlocked, null, null, createdAt, updatedAt);
    }

    /** True iff the given identity is one of the two participants. */
    public boolean hasParticipant(String userId) {
        return clientId.equals(userId) || masterId.equals(userId);
    }

    /** True iff either side has blocked the other (messaging is disabled for the pair). */
    public boolean isBlocked() {
        return clientBlocked || masterBlocked;
    }

    /**
     * The given participant's delete watermark — the messageId up to (and including) which they
     * deleted their view — or {@code null} if they never deleted (or are not a participant). History
     * and list reads floor on this for that participant.
     */
    public String deleteCursorFor(String userId) {
        if (userId.equals(clientId)) return deletedFromClient;
        if (userId.equals(masterId)) return deletedFromMaster;
        return null;
    }
}
