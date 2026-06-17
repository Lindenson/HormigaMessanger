package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

import java.util.List;
import java.util.Map;

/**
 * REST adapter (one of the inbound adapters over the universal create-chat use case; the other is
 * the Order-event consumer — M-4). All endpoints require Ory identity headers.
 */
@Path("/api/chats")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ChatResource {

    @Inject
    ConversationService conversations;

    @Inject
    TokenVerifier auth;

    public record CreateChatRequest(String clientId, String masterId, Map<String, String> metadata) {}

    @POST
    public Uni<Response> create(
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email,
            CreateChatRequest req) {

        if (auth.fromHeaders(userId, userName, role, email).isEmpty()) {
            return unauthorized();
        }
        if (req == null || req.clientId() == null || req.masterId() == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).build());
        }
        return conversations.createChat(req.clientId(), req.masterId(), req.metadata())
                .map(result -> Response
                        .status(result.created() ? Response.Status.CREATED : Response.Status.OK)
                        .entity(result.conversation())
                        .build());
    }

    @GET
    public Uni<Response> list(
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {

        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) {
            return unauthorized();
        }
        return conversations.listChats(caller.get().id())
                .map(list -> Response.ok(list).build());
    }

    @GET
    @Path("/{chatId}/messages")
    public Uni<Response> messages(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {

        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) {
            return unauthorized();
        }
        ClientData me = caller.get();
        return conversations.findById(chatId)
                .map(conv -> {
                    if (conv == null) {
                        return Response.status(Response.Status.NOT_FOUND).build();
                    }
                    if (!conv.hasParticipant(me.id())) {
                        return Response.status(Response.Status.FORBIDDEN).build();
                    }
                    // Conversation-scoped history wiring is M-5; membership-gated for now.
                    return Response.ok(List.of()).build();
                });
    }

    private Uni<Response> unauthorized() {
        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
    }
}
