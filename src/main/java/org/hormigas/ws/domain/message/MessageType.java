package org.hormigas.ws.domain.message;

public enum MessageType {
    CHAT_IN,
    CHAT_OUT,
    CHAT_ACK,

    SIGNAL_IN,
    SIGNAL_OUT,
    SIGNAL_ACK,

    // Read receipts: client → server "I read this conversation" (READ_IN, fire-and-forget),
    // server → original sender "your messages were read" (READ_OUT, fire-and-forget).
    READ_IN,
    READ_OUT,

    PRESENT_INIT,
    PRESENT_JOIN,
    PRESENT_LEAVE,

    SERVICE_OUT
}
