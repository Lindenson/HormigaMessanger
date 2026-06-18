package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.core.conversation.ConversationService.CreateResult;
import org.hormigas.ws.core.conversation.ConversationService.Outcome;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.ports.message.MessageModeration;
import org.hormigas.ws.ports.message.MessageModeration.DeleteOutcome;
import org.hormigas.ws.ports.message.ReadReceipts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("ChatResource — auth gate, membership gate, and per-endpoint status mapping")
@SuppressWarnings("unchecked")
class ChatResourceTest {

    private final ConversationService conversations = mock(ConversationService.class);
    private final TokenVerifier auth = mock(TokenVerifier.class);
    private final History<Message> history = mock(History.class);
    private final MessageModeration moderation = mock(MessageModeration.class);
    private final ReadReceipts receipts = mock(ReadReceipts.class);
    private ChatResource resource;

    private static final ClientData ALICE = new ClientData("A", "Alice", "CLIENT", "a@x");
    private static final Conversation CHAT = new Conversation(
            "chat-1", "A", "M", Map.of(), false, false, Instant.now(), Instant.now());

    @BeforeEach
    void setUp() {
        resource = new ChatResource();
        resource.conversations = conversations;
        resource.auth = auth;
        resource.history = history;
        resource.moderation = moderation;
        resource.receipts = receipts;
    }

    private void authenticated() {
        when(auth.fromHeaders(any(), any(), any(), any())).thenReturn(Optional.of(ALICE));
    }

    private void unauthenticated() {
        when(auth.fromHeaders(any(), any(), any(), any())).thenReturn(Optional.empty());
    }

    private int status(Uni<Response> u) {
        return u.await().indefinitely().getStatus();
    }

    @Test
    @DisplayName("create without identity → 401")
    void createUnauthorized() {
        unauthenticated();
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
    @DisplayName("messages: 404 when the chat is missing, 403 for a non-member, 200 for a member")
    void messagesGating() {
        authenticated();
        when(conversations.findById("missing")).thenReturn(Uni.createFrom().nullItem());
        assertEquals(404, status(resource.messages("missing", null, null, "u", "n", "r", "e")));

        Conversation other = new Conversation("c2", "X", "Y", Map.of(), false, false, Instant.now(), Instant.now());
        when(conversations.findById("c2")).thenReturn(Uni.createFrom().item(other));
        assertEquals(403, status(resource.messages("c2", null, null, "u", "n", "r", "e")));

        when(conversations.findById("chat-1")).thenReturn(Uni.createFrom().item(CHAT));
        when(history.getByConversation(eq("chat-1"), any(), anyInt())).thenReturn(Uni.createFrom().item(List.of()));
        assertEquals(200, status(resource.messages("chat-1", null, null, "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("soft-delete maps the service outcome to 204 / 404 / 403")
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
    @DisplayName("block / unblock set the blacklist flag and return 204")
    void blockUnblock() {
        authenticated();
        when(conversations.setBlocked("chat-1", "A", true)).thenReturn(Uni.createFrom().item(Outcome.OK));
        when(conversations.setBlocked("chat-1", "A", false)).thenReturn(Uni.createFrom().item(Outcome.OK));
        assertEquals(204, status(resource.block("chat-1", "u", "n", "r", "e")));
        assertEquals(204, status(resource.unblock("chat-1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("delete message: 409 when frozen, 404 when absent, 204 when deleted")
    void deleteMessageOutcomes() {
        authenticated();
        when(conversations.findById("chat-1")).thenReturn(Uni.createFrom().item(CHAT));
        when(moderation.deleteMessage("chat-1", "m1")).thenReturn(Uni.createFrom().item(DeleteOutcome.FROZEN));
        assertEquals(409, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
        when(moderation.deleteMessage("chat-1", "m1")).thenReturn(Uni.createFrom().item(DeleteOutcome.NOT_FOUND));
        assertEquals(404, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
        when(moderation.deleteMessage("chat-1", "m1")).thenReturn(Uni.createFrom().item(DeleteOutcome.DELETED));
        assertEquals(204, status(resource.deleteMessage("chat-1", "m1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("freeze requires an orderId (400) and otherwise freezes that order's messages (200)")
    void freezeRequiresOrderId() {
        authenticated();
        assertEquals(400, status(resource.freeze("chat-1", "u", "n", "r", "e", new ChatResource.FreezeRequest(" "))));

        when(conversations.findById("chat-1")).thenReturn(Uni.createFrom().item(CHAT));
        when(moderation.freezeByOrder("chat-1", "order-1")).thenReturn(Uni.createFrom().item(2));
        assertEquals(200, status(resource.freeze("chat-1", "u", "n", "r", "e",
                new ChatResource.FreezeRequest("order-1"))));
    }

    @Test
    @DisplayName("mark-read and receipts are membership-gated and return 200 for a member")
    void readAndReceipts() {
        authenticated();
        when(conversations.findById("chat-1")).thenReturn(Uni.createFrom().item(CHAT));
        when(receipts.markRead("chat-1", "A")).thenReturn(Uni.createFrom().item(1));
        when(receipts.receipts("chat-1")).thenReturn(Uni.createFrom().item(List.of()));
        assertEquals(200, status(resource.markRead("chat-1", "u", "n", "r", "e")));
        assertEquals(200, status(resource.receipts("chat-1", "u", "n", "r", "e")));
    }

    @Test
    @DisplayName("read on a chat the caller is not part of → 403")
    void readForbiddenForNonMember() {
        authenticated();
        Conversation other = new Conversation("c2", "X", "Y", Map.of(), false, false, Instant.now(), Instant.now());
        when(conversations.findById("c2")).thenReturn(Uni.createFrom().item(other));
        assertEquals(403, status(resource.markRead("c2", "u", "n", "r", "e")));
    }
}
