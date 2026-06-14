package org.hormigas.ws.infrastructure.websocket.coordinator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.session.SessionRegistry;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.websocket.coordinator.event.PresenceEventFactory;
import org.hormigas.ws.infrastructure.websocket.inbound.InboundPublisher;

import java.util.List;

@Slf4j
@ApplicationScoped
public class PresencePublisher {

    @Inject
    InboundPublisher inboundPublisher;

    @Inject
    PresenceEventFactory<Message, ClientData> eventFactory;

    public void publishInit(ClientData newClient, SessionRegistry<?> registry) {
        try {
            List<ClientData> all = registry.getAllOnlineClients();
            Message message = eventFactory.createInitMessage(all, newClient.id());
            inboundPublisher.publish(message);
            log.debug("Published INIT presence for {}", newClient.id());
        } catch (Exception e) {
            log.error("Failed to publish INIT presence", e);
        }
    }

    public void publishJoin(ClientData client) {
        try {
            Message message = eventFactory.createJoinMessage(client);
            inboundPublisher.publish(message);
            log.debug("Published JOIN presence for {}", client.id());
        } catch (Exception e) {
            log.error("Failed to publish JOIN presence", e);
        }
    }

    public void publishLeave(ClientData client) {
        try {
            Message message = eventFactory.createLeaveMessage(client);
            inboundPublisher.publish(message);
            log.debug("Published LEAVE presence for {}", client.id());
        } catch (Exception e) {
            log.error("Failed to publish LEAVE presence", e);
        }
    }
}
