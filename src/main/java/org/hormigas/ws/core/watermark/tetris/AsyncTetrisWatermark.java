package org.hormigas.ws.core.watermark.tetris;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.watermark.AsyncWatermark;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.tetris.TetrisMarker;

@Slf4j
@ApplicationScoped
public class AsyncTetrisWatermark implements AsyncWatermark {

    TetrisWatermark delegate;

    @Inject
    TetrisMarker<Message> tetrisMarker;

    @PostConstruct
    void init() {
        delegate = new TetrisWatermark(tetrisMarker);
    }

    @Override
    public void remove(String userId) {
        delegate.remove(userId)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .subscribe().with(
                        ignored -> log.debug("Client {} removed to presence", userId),
                        failure -> log.error("Failed to remove client to presence", failure)
                );
    }
}
