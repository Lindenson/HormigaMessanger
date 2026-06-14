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

@Path("/api/history")
@Produces(MediaType.APPLICATION_JSON)
public class MessageHistoryResource {


    @Inject
    History<Message> messageHistory;

    @Inject
    TokenVerifier authService;

    public static final String AUTHORIZATION = "Authorization";

    @GET
    public Uni<Response> getByClientToken(@HeaderParam(AUTHORIZATION) String authorization) {
        return Uni.createFrom().item(authService.verifyBearerToken(authorization))
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
