package org.hormigas.ws.infrastructure.identity;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.ports.identity.ClientDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * STUB {@link ClientDirectory}. Placeholder for the Ory Kratos identity read (direct admin/
 * identity API or via a thin proxy — TBD). Returns nothing for now; it exists so consumers can
 * depend on the port today and the real Kratos adapter can be dropped in later without changes
 * to the core. See twin: knowledge/concepts/messenger-functional-requirements.md (FR-SEC),
 * backlog M-1 (#16).
 */
@ApplicationScoped
public class StubClientDirectory implements ClientDirectory {

    private static final Logger log = LoggerFactory.getLogger(StubClientDirectory.class);

    @Override
    public Uni<List<ClientData>> listClients() {
        log.debug("[STUB] ClientDirectory.listClients() — Kratos integration pending; returning empty");
        return Uni.createFrom().item(List.of());
    }

    @Override
    public Uni<ClientData> findById(String id) {
        log.debug("[STUB] ClientDirectory.findById({}) — Kratos integration pending; returning null", id);
        return Uni.createFrom().nullItem();
    }
}
