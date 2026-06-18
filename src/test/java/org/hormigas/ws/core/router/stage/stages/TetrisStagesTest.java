package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.message.ReadReceipts;
import org.hormigas.ws.ports.tetris.TetrisMarker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("TetrisSentStage / TetrisAckStage — watermark tracking + ACK-driven DELIVERED")
@SuppressWarnings("unchecked")
class TetrisStagesTest {

    private final TetrisMarker<Message> marker = mock(TetrisMarker.class);
    private final ReadReceipts receipts = mock(ReadReceipts.class);

    private RouterContext<Message> ctx(Message payload) {
        return RouterContext.<Message>builder().payload(payload).pipelineType(PipelineType.INBOUND_PERSISTENT).build();
    }

    private TetrisSentStage sentStage() {
        TetrisSentStage s = new TetrisSentStage();
        s.tetrisMarker = marker;
        return s;
    }

    private TetrisAckStage ackStage() {
        TetrisAckStage s = new TetrisAckStage();
        s.tetrisMarker = marker;
        s.receipts = receipts;
        return s;
    }

    @Test
    @DisplayName("onSent is skipped when the message was not delivered")
    void sentSkippedWhenNotDelivered() {
        sentStage().apply(ctx(Message.builder().messageId("m1").build())).await().indefinitely();
        verify(marker, never()).onSent(any());
    }

    @Test
    @DisplayName("onSent records the delivered message in the watermark")
    void sentRecordsWhenDelivered() {
        Message m = Message.builder().messageId("m1").build();
        when(marker.onSent(m)).thenReturn(Uni.createFrom().item(StageResult.passed()));
        RouterContext<Message> c = ctx(m);
        c.setDelivered(StageResult.passed());
        sentStage().apply(c).await().indefinitely();
        verify(marker).onSent(m);
    }

    @Test
    @DisplayName("on ACK, advances the watermark and marks the delivered message (correlationId) DELIVERED")
    void ackAdvancesAndMarksDelivered() {
        Message ack = Message.builder().type(org.hormigas.ws.domain.message.MessageType.CHAT_ACK)
                .correlationId("delivered-id").messageId("a1").build();
        when(marker.onAck(ack)).thenReturn(Uni.createFrom().item(StageResult.passed()));
        when(receipts.markDelivered("delivered-id")).thenReturn(Uni.createFrom().item(1));

        ackStage().apply(ctx(ack)).await().indefinitely();

        verify(marker).onAck(ack);
        verify(receipts).markDelivered("delivered-id");
    }

    @Test
    @DisplayName("with no correlationId, the watermark still advances but no DELIVERED write is made")
    void ackWithoutCorrelationSkipsMarkDelivered() {
        Message ack = Message.builder().type(org.hormigas.ws.domain.message.MessageType.CHAT_ACK)
                .correlationId(null).messageId("a1").build();
        when(marker.onAck(ack)).thenReturn(Uni.createFrom().item(StageResult.passed()));

        ackStage().apply(ctx(ack)).await().indefinitely();

        verify(marker).onAck(ack);
        verify(receipts, never()).markDelivered(any());
    }
}
