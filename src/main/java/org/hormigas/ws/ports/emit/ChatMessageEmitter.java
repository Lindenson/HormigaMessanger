package org.hormigas.ws.ports.emit;

import org.hormigas.ws.domain.message.Message;

/**
 * Driven port: emit a message into the inbound delivery pipeline (the router). Implemented by the
 * transport-agnostic inbound publisher. Lets a core producer that is NOT a transport adapter — e.g. the
 * attachment-confirm use case — route a message through the SAME pipeline as a WS chat message, without
 * depending on any transport slice. Keeps the "router is the single pipeline" rule intact for
 * server-originated messages too.
 */
public interface ChatMessageEmitter {

    /**
     * Publish a message into the pipeline. Returns {@code false} if the ingress is overloaded (queue
     * full) — the message was NOT accepted and the caller should surface an overload/retry to the client.
     */
    boolean emit(Message message);
}
