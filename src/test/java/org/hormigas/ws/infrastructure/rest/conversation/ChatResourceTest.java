package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.domain.conversation.CreateResult;
import org.hormigas.ws.domain.conversation.Guarded;
import org.hormigas.ws.domain.conversation.Outcome;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.ports.message.MessageModeration.DeleteOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * ChatResource is a thin adapter: it authenticates and maps the core {@link Guarded} outcome to HTTP.
 * Membership/authorization is enforced in {@link Chats} (covered by ConversationsTest), so this
 * test only verifies the auth gate and the per-endpoint status mapping.
 */
@DisplayName("ChatResource — auth gate + Guarded→HTTP mapping")
class ChatResourceTest {

    private final Chats conversations = mock(Chats.class);
    private final TokenVerifier auth = mock(TokenVerifier.class);
    private ChatResource resource;

    private static final ClientData ALICE = new ClientData("A", "Alice", "CLIENT", "a@x");
    private static final Conversation CHAT = new Conversation(
            "chat-1", "A", "M", Map.of(), false, false, Instant.now(), Instant.now());

    @BeforeEach
    void setUp() {
        resource = new ChatResource();
        resource.conversations = conversations;
        resource.auth = auth;
    }

    private void authenticated() {
        when(auth.fromHeaders(any(), any(), any(), any())).thenReturn(Optional.of(ALICE));
    }

    private int status(Uni<Response> u) {
        return u.await().indefinitely().getStatus();
    }

    @Test
    @DisplayName("create without identity → 401")
    void createUnauthorized() {
        when(auth.fromHeaders(any(), any(), any(), any())).thenReturn(Optional.empty());
        assertEquals(401, status(resource.create("u", "n", "r", "e",
                new ChatResource.CreateChatRequest("A", "M", Map.of()))));
    }

    @Test
    @DisplayName("create with a missing participant → 400")
    void createBadRequest() {
        authenticated();
        assertEquals(400, status(resource.create("u", "n", "r", "e",
                new ChatResource.CreateChatRequest(null, "M", Map.of()))));
    }

    @Test
    @DisplayName("create a new chat → 201, an existing one → 200 (idempotent)")
    void createNewVsExisting() {
        authenticated();
        when(conversations.createChat("A", "M", Map.of()))
                .thenReturn(Uni.createFrom().item(new CreateResult(CHAT, true)));
        assertEquals(201, status(resource.create("u", "n", "r", "e",
                new ChatResource.CreateChatRequest("A", "M", Map.of()))));

        when(conversations.createChat("A", "M", Map.of()))
                .thenReturn(Uni.createFrom().item(new CreateResult(CHAT, false)));
        assertEquals(200, status(resource.create("u", "n", "r", "e",
                new ChatResource.CreateChatRequest("A", "M", Map.of()))));
    }

    @Test
    @DisplayName("list chats → 200 for an authenticated caller")
    void listOk() {
        authenticated();
        when(conversations.listChats("A")).thenReturn(Uni.createFrom().item(List.of(CHAT)));
        assertEquals(200, status(resource.list("u", "n", "r", "e")));
    }

    @Test
    @DisplayName("messages: maps Guarded NOT_FOUND→404, FORBIDDEN→403, OK→200")
    void messagesGating() {
        authenticated();
        when(conversations.history(eq("missing"), eq("A"), any(), any()))
                .thenReturn(Uni.createFrom().item(Guarded.notFound()));
        assertEquals(404, status(resource.messages("missing", null, null, "u", "n", "r", "e")));

        when(conversations.history(eq("c2"), eq("A"), any(), any()))
                .thenReturn(Uni.createFrom().item(Guarded.forbidden()));
        assertEquals(403, status(resource.messages("c2", null, null, "u", "n", "r", "e")));

        when(conversations.history(eq("chat-1"), eq("A"), any(), any()))
                .thenReturn(Uni.createFrom().item(Guarded.ok(List.of())));
        assertEquals(200, status(resource.messages("chat-1", null, null, "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("soft-delete maps the outcome to 204 / 404 / 403")
    void softDeleteOutcomes() {
        authenticated();
        when(conversations.hide("chat-1", "A")).thenReturn(Uni.createFrom().item(Outcome.OK));
        assertEquals(204, status(resource.softDelete("chat-1", "u", "n", "r", "e")));
        when(conversations.hide("chat-1", "A")).thenReturn(Uni.createFrom().item(Outcome.NOT_FOUND));
        assertEquals(404, status(resource.softDelete("chat-1", "u", "n", "r", "e")));
        when(conversations.hide("chat-1", "A")).thenReturn(Uni.createFrom().item(Outcome.FORBIDDEN));
        assertEquals(403, status(resource.softDelete("chat-1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("block / unblock return 204")
    void blockUnblock() {
        authenticated();
        when(conversations.setBlocked("chat-1", "A", true)).thenReturn(Uni.createFrom().item(Outcome.OK));
        when(conversations.setBlocked("chat-1", "A", false)).thenReturn(Uni.createFrom().item(Outcome.OK));
        assertEquals(204, status(resource.block("chat-1", "u", "n", "r", "e")));
        assertEquals(204, status(resource.unblock("chat-1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("delete message: OK+FROZEN→409, OK+DELETED→204, guard FORBIDDEN→403")
    void deleteMessageOutcomes() {
        authenticated();
        when(conversations.deleteMessage("chat-1", "A", "m1"))
                .thenReturn(Uni.createFrom().item(Guarded.ok(DeleteOutcome.FROZEN)));
        assertEquals(409, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
        when(conversations.deleteMessage("chat-1", "A", "m1"))
                .thenReturn(Uni.createFrom().item(Guarded.ok(DeleteOutcome.DELETED)));
        assertEquals(204, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
        when(conversations.deleteMessage("chat-1", "A", "m1"))
                .thenReturn(Uni.createFrom().item(Guarded.forbidden()));
        assertEquals(403, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("freeze requires an orderId (400), otherwise maps the guarded count to 200")
    void freezeRequiresOrderId() {
        authenticated();
        assertEquals(400, status(resource.freeze("chat-1", "u", "n", "r", "e", new ChatResource.FreezeRequest(" "))));

        when(conversations.freezeByOrder("chat-1", "A", "order-1"))
                .thenReturn(Uni.createFrom().item(Guarded.ok(2)));
        assertEquals(200, status(resource.freeze("chat-1", "u", "n", "r", "e",
                new ChatResource.FreezeRequest("order-1"))));
    }

    @Test
    @DisplayName("mark-read and receipts return 200 for a member, 403 for a non-member")
    void readAndReceipts() {
        authenticated();
        when(conversations.markRead("chat-1", "A")).thenReturn(Uni.createFrom().item(Guarded.ok(1)));
        when(conversations.receipts("chat-1", "A")).thenReturn(Uni.createFrom().item(Guarded.ok(List.of())));
        assertEquals(200, status(resource.markRead("chat-1", "u", "n", "r", "e")));
        assertEquals(200, status(resource.receipts("chat-1", "u", "n", "r", "e")));

        when(conversations.markRead("c2", "A")).thenReturn(Uni.createFrom().item(Guarded.forbidden()));
        assertEquals(403, status(resource.markRead("c2", "u", "n", "r", "e")));
    }
}
