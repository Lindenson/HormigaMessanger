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
    // deleteOlderThan
    // ----------------------------------------------------------------------
    Uni<Integer> deleteOlderThan(Instant threshold);

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
