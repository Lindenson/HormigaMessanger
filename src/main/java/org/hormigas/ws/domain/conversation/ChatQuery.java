package org.hormigas.ws.domain.conversation;

import java.time.Instant;

/**
 * Filter/sort/page criteria for an admin chat listing (UC admin, scope B). Every field is optional
 * except paging: a {@code null} filter is not applied. {@code participantId} matches either side of
 * the pair. Framework-free domain value.
 *
 * @param participantId  match chats where this id is the client OR the master (null = any)
 * @param conversationId match a single chat by id (null = any)
 * @param blockedOnly    if true, only chats blocked by either side
 * @param createdFrom    lower bound (inclusive) on created_at (null = open)
 * @param createdTo      upper bound (exclusive) on created_at (null = open)
 * @param sort           ordering (null defaults to {@link ChatSort#UPDATED_DESC} in the core)
 * @param limit          page size (clamped in the core)
 * @param offset         page offset, >= 0
 */
public record ChatQuery(
        String participantId,
        String conversationId,
        boolean blockedOnly,
        Instant createdFrom,
        Instant createdTo,
        ChatSort sort,
        int limit,
        int offset
) {}
