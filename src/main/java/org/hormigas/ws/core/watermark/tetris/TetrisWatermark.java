package org.hormigas.ws.core.watermark.tetris;

import io.smallrye.mutiny.Uni;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.watermark.Watermark;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.tetris.TetrisMarker;


@Slf4j
@RequiredArgsConstructor
public class TetrisWatermark implements Watermark {

    private final TetrisMarker<Message> tetrisMarker;

    @Override
    public Uni<Void> remove(String userId) {
        return tetrisMarker.onDisconnect(userId)
                .onItem().transformToUni(result -> {
                    if (result.isSuccess()) log.debug("Client {} removed from tetris", userId);
                    else log.warn("Client {} was now from presence", userId);
                    return Uni.createFrom().voidItem();
                }).onFailure().invoke(failure -> log.error("Failed to remove client {} from tetris", userId, failure))
                .onFailure().recoverWithUni(Uni.createFrom().voidItem());
    }
}
