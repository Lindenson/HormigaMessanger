package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.ports.message.MessageModeration;
import org.hormigas.ws.ports.message.ReadReceipts;

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

    @Inject
    History<Message> history;

    @Inject
    MessageModeration moderation;

    @Inject
    ReadReceipts receipts;

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
                .flatMap(conv -> {
                    if (conv == null) {
                        return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
                    }
                    if (!conv.hasParticipant(me.id())) {
                        return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
                    }
                    // Conversation-scoped history sync (UC-U50 / reconnect durability).
                    return history.getByConversation(chatId)
                            .map(messages -> Response.ok(messages).build());
                });
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
        String meId = caller.get().id();
        return conversations.findById(chatId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
            if (!conv.hasParticipant(meId)) return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
            return moderation.deleteMessage(chatId, messageId).map(outcome -> switch (outcome) {
                case DELETED -> Response.noContent().build();
                case NOT_FOUND -> Response.status(Response.Status.NOT_FOUND).build();
                case FROZEN -> Response.status(Response.Status.CONFLICT).build(); // 409: frozen, immutable
            });
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
        String meId = caller.get().id();
        return conversations.findById(chatId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
            if (!conv.hasParticipant(meId)) return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
            return moderation.freezeByOrder(chatId, req.orderId())
                    .map(n -> Response.ok(Map.of("frozen", n)).build());
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
        String meId = caller.get().id();
        return conversations.findById(chatId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
            if (!conv.hasParticipant(meId)) return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
            // the caller acknowledges the messages addressed to them (UC-U14)
            return receipts.markRead(chatId, meId)
                    .map(n -> Response.ok(Map.of("read", n)).build());
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
        String meId = caller.get().id();
        return conversations.findById(chatId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
            if (!conv.hasParticipant(meId)) return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
            return receipts.receipts(chatId).map(list -> Response.ok(list).build());
        });
    }

    private Response toResponse(ConversationService.Outcome outcome) {
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
