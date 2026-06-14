package org.hormigas.ws.domain.message;

public enum MessageType {
    CHAT_IN,
    CHAT_OUT,
    CHAT_ACK,

    SIGNAL_IN,
    SIGNAL_OUT,
    SIGNAL_ACK,

    PRESENT_INIT,
    PRESENT_JOIN,
    PRESENT_LEAVE,

    SERVICE_OUT
}
