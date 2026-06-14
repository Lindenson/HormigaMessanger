package org.hormigas.ws.infrastructure.websocket.coordinator.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.credentials.ClientData;

import java.util.List;

@ApplicationScoped
@RequiredArgsConstructor
public class MessageEventFactory implements PresenceEventFactory<Message, ClientData> {

    private final ObjectMapper objectMapper;

    @Override
    public Message createInitMessage(List<ClientData> clients, String recipientId) throws Exception {
        String body = objectMapper.writeValueAsString(clients);
        return baseMessage(MessageType.PRESENT_INIT, body, recipientId);
    }

    @Override
    public Message createJoinMessage(ClientData clientData) throws Exception {
        String body = objectMapper.writeValueAsString(clientData);
        return baseMessage(MessageType.PRESENT_JOIN, body, BROADCAST);
    }

    @Override
    public Message createLeaveMessage(ClientData clientData) throws Exception {
        String body = objectMapper.writeValueAsString(clientData);
        return baseMessage(MessageType.PRESENT_LEAVE, body, BROADCAST);
    }

    private Message baseMessage(MessageType type, String body, String recipientId) {
        return Message.builder()
                .type(type)
                .payload(Message.Payload.builder().kind("presence").body(body).build())
                .recipientId(recipientId)
                .senderId("server")
                .build();
    }
}
