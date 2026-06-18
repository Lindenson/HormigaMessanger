package org.hormigas.ws.core.watermark.tetris;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.tetris.TetrisMarker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

@DisplayName("TetrisWatermark — remove delegates to the marker's onDisconnect and never fails")
@SuppressWarnings("unchecked")
class TetrisWatermarkTest {

    private final TetrisMarker<Message> marker = mock(TetrisMarker.class);
    private final TetrisWatermark watermark = new TetrisWatermark(marker);

    @Test
    @DisplayName("remove delegates the client to TetrisMarker.onDisconnect")
    void removeDelegates() {
        when(marker.onDisconnect("client-1")).thenReturn(Uni.createFrom().item(StageResult.passed()));
        watermark.remove("client-1").await().indefinitely();
        verify(marker).onDisconnect("client-1");
    }

    @Test
    @DisplayName("a marker failure is swallowed — remove completes without error")
    void removeRecoversFromFailure() {
        when(marker.onDisconnect("client-1"))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("redis down")));
        // completes (returns void) instead of propagating the failure
        watermark.remove("client-1").await().indefinitely();
        verify(marker).onDisconnect("client-1");
    }
}
