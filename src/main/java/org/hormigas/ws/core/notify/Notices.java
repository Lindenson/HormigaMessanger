package org.hormigas.ws.core.notify;

import io.smallrye.mutiny.Uni;

import java.util.Map;

/**
 * Driving port (port-IN) for emitting must-arrive system notices (Strategy C, ADR-014). Inbound
 * adapters (the admin REST trigger, service-to-service callers) depend on this interface; the core
 * ({@code core.notify.SystemNotifier}) implements it. The outside reaches the core only through the port.
 */
public interface Notices {

    /** Emit a system notice to {@code recipientId} (records the dead-letter draft, then delivers). */
    Uni<Void> notify(String recipientId, String kind, String body, Map<String, String> meta, String conversationId);
}
