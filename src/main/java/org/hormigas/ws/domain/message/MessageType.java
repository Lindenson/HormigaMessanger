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

    SERVICE_OUT,

    // Strategy C — must-arrive system notices (ADR-014). SYSTEM_OUT is server→client, durable in the
    // dead_letter draft + delivered via the transient outbox path; SYSTEM_ACK is the client's
    // delivery confirmation (correlationId = the notice's messageId) that retracts the draft.
    SYSTEM_OUT,
    SYSTEM_ACK
}
