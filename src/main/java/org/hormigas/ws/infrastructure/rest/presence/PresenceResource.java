package org.hormigas.ws.infrastructure.rest.presence;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.hormigas.ws.domain.presence.OnlineClient;
import org.hormigas.ws.ports.presence.PresenceManager;

import java.util.List;

@Path("/api/presence")
@Produces(MediaType.APPLICATION_JSON)
public class PresenceResource {

    @Inject
    PresenceManager presenceManager;

    @GET
    public Uni<List<OnlineClient>> getPresence() {
        return presenceManager.getAll();
    }
}
