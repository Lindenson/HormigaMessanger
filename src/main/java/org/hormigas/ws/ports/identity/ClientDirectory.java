package org.hormigas.ws.ports.identity;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.credentials.ClientData;

import java.util.List;

/**
 * Driven port — the source of truth for WHO the clients/identities are.
 *
 * <p>Per-request identity arrives from the edge (Ory) headers/token; this port is the
 * separate question of enumerating / looking up identities (e.g. to resolve a participant's
 * display data when creating a chat, or an admin listing).
 *
 * <p>Backed today by a stub ({@code infrastructure/identity}); the production adapter will
 * read from <b>Ory Kratos</b> — either directly (admin/identity API) or via a thin proxy.
 * Triggers/consumers depend only on this interface, so the source can change without touching
 * the core (hexagonal driven port).
 */
public interface ClientDirectory {

    /** All known clients/identities. */
    Uni<List<ClientData>> listClients();

    /** One identity by id; emits {@code null} if not found. */
    Uni<ClientData> findById(String id);
}
