package org.hormigas.ws.infrastructure.rest.history;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

@Path("/api/history")
@Produces(MediaType.APPLICATION_JSON)
public class MessageHistoryResource {


    @Inject
    History<Message> messageHistory;

    @Inject
    TokenVerifier authService;

    @GET
    public Uni<Response> getByClient(
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        return Uni.createFrom().item(authService.fromHeaders(userId, userName, role, email))
                .onItem().transformToUni(clientOpt -> {
                    if (clientOpt.isEmpty()) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    String clientId = clientOpt.get().id();
                    return messageHistory.getAllBySenderId(clientId)
                            .onItem().transform(messages -> Response.ok(messages).build());
                });
    }
}
