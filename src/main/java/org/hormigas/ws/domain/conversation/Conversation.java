package org.hormigas.ws.domain.conversation;

import java.time.Instant;
import java.util.Map;

/**
 * A 1:1 master↔client chat, keyed by the participant pair (clientId, masterId).
 * Order-agnostic: {@code metadata} is opaque (carries the order reference for the frontend).
 * Framework-free domain record.
 */
public record Conversation(
        String id,
        String clientId,
        String masterId,
        Map<String, String> metadata,
        Instant createdAt,
        Instant updatedAt
) {
    /** True iff the given identity is one of the two participants. */
    public boolean hasParticipant(String userId) {
        return clientId.equals(userId) || masterId.equals(userId);
    }
}
