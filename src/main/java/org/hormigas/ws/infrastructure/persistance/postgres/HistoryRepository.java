package org.hormigas.ws.infrastructure.persistance.postgres;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;

import java.time.Instant;
import java.util.List;

public interface HistoryRepository {
    // ----------------------------------------------------------------------
    // insertHistoryBatch
    // ----------------------------------------------------------------------
    Uni<List<Inserted>> insertHistoryBatch(List<HistoryRow> batch);

    // ----------------------------------------------------------------------
    // deleteByIds
    // ----------------------------------------------------------------------
    Uni<Integer> deleteByIds(List<Long> ids);

    // ----------------------------------------------------------------------
    // deleteByMessageIds
    // ----------------------------------------------------------------------
    Uni<Integer> deleteByMessageIds(List<String> messageIds);

    // ----------------------------------------------------------------------
    // deleteOlderThan — normal retention class ONLY (UC-U23, decision #6).
    // Frozen messages are a separate, longer retention class and are NOT purged here; use
    // deleteFrozenOlderThan with the longer threshold instead.
    // ----------------------------------------------------------------------
    Uni<Integer> deleteOlderThan(Instant threshold);

    // ----------------------------------------------------------------------
    // deleteFrozenOlderThan — frozen retention class (≈ order-retention, longer than normal).
    // ----------------------------------------------------------------------
    Uni<Integer> deleteFrozenOlderThan(Instant threshold);

    // ----------------------------------------------------------------------
    // findAllByConversationId
    // ----------------------------------------------------------------------
    Uni<List<HistoryRow>> findAllByConversationId(String conversationId);

    // ----------------------------------------------------------------------
    // findAllBySenderId
    // ----------------------------------------------------------------------
    Uni<List<HistoryRow>> findAllBySenderId(String senderId);

    // ----------------------------------------------------------------------
    // findAllByRecipientId
    // ----------------------------------------------------------------------
    Uni<List<HistoryRow>> findAllByRecipientId(String recipientId);

    // ----------------------------------------------------------------------
    // findAllByParticipantId (sender OR recipient)
    // ----------------------------------------------------------------------
    Uni<List<HistoryRow>> findAllByParticipantId(String participantId);
}
