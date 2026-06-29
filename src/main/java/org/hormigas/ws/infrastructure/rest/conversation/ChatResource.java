package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.domain.conversation.Guarded;
import org.hormigas.ws.domain.conversation.Outcome;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

import java.util.Map;

/**
 * REST adapter over the conversation use cases. Every endpoint is thin: it authenticates the Ory
 * identity headers, delegates to the {@link Chats} core use case, and renders the domain
 * result to HTTP. Membership/authorization and all business logic live in {@code core} (the
 * {@code guardedRead} guard), never here — this resource only adapts HTTP to the use cases and back.
 */
@Path("/api/chats")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ChatResource {

    @Inject
    Chats conversations;

    @Inject
    TokenVerifier auth;

    public record CreateChatRequest(String clientId, String masterId, Map<String, String> metadata) {}

    public record FreezeRequest(String orderId) {}

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
            @QueryParam("since") String since,
            @QueryParam("limit") Integer limit,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {

        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) {
            return unauthorized();
        }
        // Conversation-scoped, cursor-paginated history sync (UC-U50). Guard + clamping live in core.
        return conversations.history(chatId, caller.get().id(), since, limit)
                .map(this::guardedToResponse);
    }

    @DELETE
    @Path("/{chatId}")
    public Uni<Response> softDelete(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return conversations.hide(chatId, caller.get().id()).map(this::toResponse);
    }

    @POST
    @Path("/{chatId}/block")
    public Uni<Response> block(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return conversations.setBlocked(chatId, caller.get().id(), true).map(this::toResponse);
    }

    @DELETE
    @Path("/{chatId}/block")
    public Uni<Response> unblock(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return conversations.setBlocked(chatId, caller.get().id(), false).map(this::toResponse);
    }

    @DELETE
    @Path("/{chatId}/messages/{messageId}")
    public Uni<Response> deleteMessage(
            @PathParam("chatId") String chatId,
            @PathParam("messageId") String messageId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return conversations.deleteMessage(chatId, caller.get().id(), messageId)
                .map(g -> switch (g.outcome()) {
                    case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
                    case FORBIDDEN -> Response.status(Response.Status.FORBIDDEN).build();
                    case OK -> switch (g.value()) {
                        case DELETED -> Response.noContent().build();
                        case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
                        case FROZEN -> Response.status(Response.Status.CONFLICT).build(); // 409: frozen, immutable
                    };
                });
    }

    @POST
    @Path("/{chatId}/freeze")
    public Uni<Response> freeze(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email,
            FreezeRequest req) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        // Freeze is message-level, scoped by orderId (UC-U22) — there is no chat-wide freeze.
        if (req == null || req.orderId() == null || req.orderId().isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).build());
        }
        return conversations.freezeByOrder(chatId, caller.get().id(), req.orderId())
                .map(g -> switch (g.outcome()) {
                    case OK -> Response.ok(Map.of("frozen", g.value())).build();
                    case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
                    case FORBIDDEN -> Response.status(Response.Status.FORBIDDEN).build();
                });
    }

    @POST
    @Path("/{chatId}/read")
    public Uni<Response> markRead(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        // REST fallback for the READ_IN WebSocket event (UC-U14); the caller acks messages addressed to them.
        return conversations.markRead(chatId, caller.get().id())
                .map(g -> switch (g.outcome()) {
                    case OK -> Response.ok(Map.of("read", g.value())).build();
                    case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
                    case FORBIDDEN -> Response.status(Response.Status.FORBIDDEN).build();
                });
    }

    @GET
    @Path("/{chatId}/receipts")
    public Uni<Response> receipts(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return conversations.receipts(chatId, caller.get().id()).map(this::guardedToResponse);
    }

    /** Render a membership-guarded read result to HTTP: OK→200 body, NOT_FOUND→404, FORBIDDEN→403. */
    private <T> Response guardedToResponse(Guarded<T> g) {
        return switch (g.outcome()) {
            case OK -> Response.ok(g.value()).build();
            case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
            case FORBIDDEN -> Response.status(Response.Status.FORBIDDEN).build();
        };
    }

    private Response toResponse(Outcome outcome) {
        return switch (outcome) {
            case OK -> Response.noContent().build();
            case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
            case FORBIDDEN -> Response.status(Response.Status.FORBIDDEN).build();
        };
    }

    private Uni<Response> unauthorized() {
        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
    }
}
