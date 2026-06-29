package org.hormigas.ws.infrastructure.cache.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

@DisplayName("CachedConversationDirectory — L1 cache: load-on-miss, serve-from-cache, invalidate")
class CachedConversationDirectoryTest {

    private ConversationManager manager;
    private CachedConversationDirectory directory;

    private static final Conversation CHAT = new Conversation(
            "c1", "A", "M", Map.of(), false, false, Instant.now(), Instant.now());

    @BeforeEach
    void setUp() {
        manager = mock(ConversationManager.class);
        directory = new CachedConversationDirectory();
        directory.manager = manager;
        directory.maxSize = 100;
        directory.ttlSeconds = 60;
        directory.init();
    }

    @Test
    @DisplayName("a miss loads from the manager; a second lookup is served from the cache (one DB hit)")
    void loadOnMissThenServeFromCache() {
        when(manager.findById("c1")).thenReturn(Uni.createFrom().item(CHAT));

        assertEquals(CHAT, directory.findById("c1").await().indefinitely());
        assertEquals(CHAT, directory.findById("c1").await().indefinitely());

        verify(manager, times(1)).findById("c1");
    }

    @Test
    @DisplayName("invalidate evicts — the next lookup reloads from the manager")
    void invalidateForcesReload() {
        when(manager.findById("c1")).thenReturn(Uni.createFrom().item(CHAT));

        directory.findById("c1").await().indefinitely();
        directory.invalidate("c1");
        directory.findById("c1").await().indefinitely();

        verify(manager, times(2)).findById("c1");
    }

    @Test
    @DisplayName("an absent conversation is not negatively cached — it is reloaded each time")
    void absentNotCached() {
        when(manager.findById("missing")).thenReturn(Uni.createFrom().nullItem());

        assertNull(directory.findById("missing").await().indefinitely());
        assertNull(directory.findById("missing").await().indefinitely());

        verify(manager, times(2)).findById("missing");
    }
}
