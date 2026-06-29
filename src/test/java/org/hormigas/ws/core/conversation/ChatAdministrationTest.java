package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.ChatQuery;
import org.hormigas.ws.domain.conversation.ChatSort;
import org.hormigas.ws.domain.conversation.ChatStats;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("ChatAdministration — admin read use cases: page clamping, sort default, delegation")
class ChatAdministrationTest {

    private ConversationManager manager;
    private ChatAdministration admin;

    @BeforeEach
    void setUp() {
        manager = mock(ConversationManager.class);
        admin = new ChatAdministration();
        admin.manager = manager;
        when(manager.findAll(any())).thenReturn(Uni.createFrom().item(List.of()));
        when(manager.count(any())).thenReturn(Uni.createFrom().item(0L));
        when(manager.stats()).thenReturn(Uni.createFrom().item(new ChatStats(0, 0)));
    }

    private static ChatQuery q(int limit, int offset, ChatSort sort) {
        return new ChatQuery(null, null, false, null, null, sort, limit, offset);
    }

    private ChatQuery captureListQuery() {
        var captor = org.mockito.ArgumentCaptor.forClass(ChatQuery.class);
        verify(manager).findAll(captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("a non-positive limit becomes the default; the sort defaults to UPDATED_DESC")
    void appliesDefaults() {
        admin.list(q(0, 0, null)).await().indefinitely();
        ChatQuery sent = captureListQuery();
        assertEquals(ChatAdministration.DEFAULT_LIMIT, sent.limit());
        assertEquals(ChatSort.UPDATED_DESC, sent.sort());
        assertEquals(0, sent.offset());
    }

    @Test
    @DisplayName("a too-large limit is clamped to the max; a negative offset becomes 0")
    void clampsLimitAndOffset() {
        admin.list(q(10_000, -5, ChatSort.CREATED_ASC)).await().indefinitely();
        ChatQuery sent = captureListQuery();
        assertEquals(ChatAdministration.MAX_LIMIT, sent.limit());
        assertEquals(0, sent.offset());
        assertEquals(ChatSort.CREATED_ASC, sent.sort(), "an explicit sort is preserved");
    }

    @Test
    @DisplayName("count normalizes the same way and delegates")
    void countDelegates() {
        when(manager.count(any())).thenReturn(Uni.createFrom().item(42L));
        assertEquals(42L, admin.count(q(0, 0, null)).await().indefinitely());
        var captor = org.mockito.ArgumentCaptor.forClass(ChatQuery.class);
        verify(manager).count(captor.capture());
        assertEquals(ChatAdministration.DEFAULT_LIMIT, captor.getValue().limit());
    }

    @Test
    @DisplayName("stats passes through unchanged")
    void statsDelegates() {
        when(manager.stats()).thenReturn(Uni.createFrom().item(new ChatStats(7, 2)));
        ChatStats s = admin.stats().await().indefinitely();
        assertEquals(7, s.total());
        assertEquals(2, s.blocked());
    }
}
