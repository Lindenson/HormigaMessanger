package org.hormigas.ws.infrastructure.websocket.outbound.transformers;

import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.session.ClientSession;

@ApplicationScoped
public class CreditsTransformer implements Transformer<Message, WebSocketConnection> {
    @Override
    public Message apply(@Nullable Message message, @Nonnull ClientSession<WebSocketConnection> clientSession) {
        if (message == null) return null;

        int credits = (int) Math.floor(clientSession.getAvailableCredits());
        return message.toBuilder().creditsAvailable(credits).build();
    }
}
