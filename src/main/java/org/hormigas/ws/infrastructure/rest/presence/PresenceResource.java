package org.hormigas.ws.infrastructure.rest.presence;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;
import org.hormigas.ws.ports.presence.PresenceManager;

@Path("/api/presence")
@Produces(MediaType.APPLICATION_JSON)
public class PresenceResource {

    @Inject
    PresenceManager presenceManager;

    @Inject
    TokenVerifier auth;

    @GET
    public Uni<Response> getPresence(
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        if (auth.fromHeaders(userId, userName, role, email).isEmpty()) {
            return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
        }
        return presenceManager.getAll().map(list -> Response.ok(list).build());
    }
}
