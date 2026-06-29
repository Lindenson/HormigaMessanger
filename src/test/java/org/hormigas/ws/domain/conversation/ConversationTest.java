package org.hormigas.ws.domain.conversation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Conversation domain — participants, block (mutual) and the per-side delete watermark")
class ConversationTest {

    private static final Instant T = Instant.parse("2026-06-29T00:00:00Z");

    private static Conversation chat(boolean cBlocked, boolean mBlocked, String delClient, String delMaster) {
        return new Conversation("c1", "client", "master", Map.of(),
                cBlocked, mBlocked, delClient, delMaster, T, T);
    }

    @Test
    @DisplayName("the 8-arg convenience constructor leaves both watermarks null (never deleted)")
    void convenienceConstructorHasNoWatermarks() {
        Conversation c = new Conversation("c1", "client", "master", Map.of(), false, false, T, T);
        assertNull(c.deletedFromClient());
        assertNull(c.deletedFromMaster());
        assertNull(c.deleteCursorFor("client"));
    }

    @Test
    @DisplayName("hasParticipant recognises both participants and rejects strangers")
    void hasParticipant() {
        Conversation c = chat(false, false, null, null);
        assertTrue(c.hasParticipant("client"));
        assertTrue(c.hasParticipant("master"));
        assertFalse(c.hasParticipant("stranger"));
    }

    @Test
    @DisplayName("a block by either side disables messaging for the pair (mutual)")
    void blockIsMutual() {
        assertTrue(chat(true, false, null, null).isBlocked());
        assertTrue(chat(false, true, null, null).isBlocked());
        assertFalse(chat(false, false, null, null).isBlocked());
    }

    @Test
    @DisplayName("deleteCursorFor returns each side's own watermark (per-participant, null for strangers)")
    void deleteCursorIsPerParticipant() {
        Conversation c = chat(false, false, "01MSG-CLIENT", null); // client deleted up to a point
        assertEquals("01MSG-CLIENT", c.deleteCursorFor("client"));
        assertNull(c.deleteCursorFor("master"), "the peer who did not delete has no floor");
        assertNull(c.deleteCursorFor("stranger"));
    }
}
