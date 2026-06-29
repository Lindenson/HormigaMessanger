package org.hormigas.ws.infrastructure.rest.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.conversation.AdminChats;
import org.hormigas.ws.domain.conversation.ChatQuery;
import org.hormigas.ws.domain.conversation.ChatStats;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("AdminChatResource — ADMIN-only gate, query parsing, delegation")
class AdminChatResourceTest {

    private AdminChats adminChats;
    private TokenVerifier auth;
    private AdminChatResource resource;

    @BeforeEach
    void setUp() {
        adminChats = mock(AdminChats.class);
        auth = mock(TokenVerifier.class);
        resource = new AdminChatResource();
        resource.adminChats = adminChats;
        resource.auth = auth;
        when(auth.fromHeaders(any(), any(), any(), any()))
                .thenReturn(Optional.of(new ClientData("admin-1", "Admin", "ADMIN", "a@x")));
        when(adminChats.list(any())).thenReturn(Uni.createFrom().item(List.<Conversation>of()));
        when(adminChats.count(any())).thenReturn(Uni.createFrom().item(0L));
        when(adminChats.stats()).thenReturn(Uni.createFrom().item(new ChatStats(0, 0)));
    }

    private int status(Uni<Response> u) {
        return u.await().indefinitely().getStatus();
    }

    private Uni<Response> list(String role, String from, String sort) {
        return resource.list(null, null, false, from, null, sort, 0, 0, "u", "n", role, "e");
    }

    @Test
    @DisplayName("no identity → 401")
    void unauthenticated() {
        when(auth.fromHeaders(any(), any(), any(), any())).thenReturn(Optional.empty());
        assertEquals(401, status(list("ADMIN", null, null)));
    }

    @Test
    @DisplayName("a non-ADMIN caller (e.g. SERVICE/CLIENT) → 403")
    void nonAdminForbidden() {
        assertEquals(403, status(list("SERVICE", null, null)));
        assertEquals(403, status(list("CLIENT", null, null)));
        assertEquals(403, status(resource.stats("u", "n", "MASTER", "e")));
        verifyNoInteractions(adminChats);
    }

    @Test
    @DisplayName("ADMIN list → 200 and delegates")
    void adminListOk() {
        assertEquals(200, status(list("ADMIN", null, null)));
        verify(adminChats).list(any());
        verify(adminChats).count(any());
    }

    @Test
    @DisplayName("filters are passed through to the query (blocked + participant + sort)")
    void filtersPassThrough() {
        resource.list("client-9", null, true, null, null, "created_asc", 25, 5, "u", "n", "ADMIN", "e")
                .await().indefinitely();
        var captor = org.mockito.ArgumentCaptor.forClass(ChatQuery.class);
        verify(adminChats).list(captor.capture());
        ChatQuery q = captor.getValue();
        assertEquals("client-9", q.participantId());
        assertTrue(q.blockedOnly());
        assertEquals(org.hormigas.ws.domain.conversation.ChatSort.CREATED_ASC, q.sort());
        assertEquals(25, q.limit());
        assertEquals(5, q.offset());
    }

    @Test
    @DisplayName("a malformed timestamp → 400 (and the core is not called)")
    void badTimestamp() {
        assertEquals(400, status(list("ADMIN", "not-a-date", null)));
        verify(adminChats, never()).list(any());
    }

    @Test
    @DisplayName("an unknown sort → 400")
    void badSort() {
        assertEquals(400, status(list("ADMIN", null, "sideways")));
        verify(adminChats, never()).list(any());
    }

    @Test
    @DisplayName("ADMIN stats → 200")
    void statsOk() {
        when(adminChats.stats()).thenReturn(Uni.createFrom().item(new ChatStats(5, 1)));
        assertEquals(200, status(resource.stats("u", "n", "ADMIN", "e")));
    }
}
