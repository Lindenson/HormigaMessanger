package org.hormigas.ws.infrastructure.websocket.coordinator.event;

import java.util.List;

public interface PresenceEventFactory<T, S> {

    String BROADCAST = "BROADCAST";

    T createInitMessage(List<S> clients, String recipientId) throws Exception;
    T createJoinMessage(S clientData) throws Exception;
    T createLeaveMessage(S clientData) throws Exception;
}
