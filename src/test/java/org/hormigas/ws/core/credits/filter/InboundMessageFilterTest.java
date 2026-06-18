package org.hormigas.ws.core.credits.filter;

import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.session.ClientSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@DisplayName("InboundMessageFilter — null guard + credit-based rate limiting")
@SuppressWarnings("unchecked")
class InboundMessageFilterTest {

    private final InboundMessageFilter<String> filter = new InboundMessageFilter<>();
    private final ClientSession<String> session = mock(ClientSession.class);

    @Test
    @DisplayName("a null message is rejected")
    void rejectsNull() {
        assertFalse(filter.filter(null, session));
    }

    @Test
    @DisplayName("a creditable message passes when credits are available")
    void passesWhenCreditsAvailable() {
        when(session.tryConsumeCredits()).thenReturn(true);
        assertTrue(filter.filter(Message.builder().type(MessageType.CHAT_IN).build(), session));
    }

    @Test
    @DisplayName("a creditable message is rejected when the client is over its credit limit")
    void rejectsWhenOverLimit() {
        when(session.tryConsumeCredits()).thenReturn(false);
        assertFalse(filter.filter(Message.builder().type(MessageType.CHAT_IN).build(), session));
    }

    @Test
    @DisplayName("an ACK bypasses credit accounting and is never charged")
    void ackBypassesCredits() {
        assertTrue(filter.filter(Message.builder().type(MessageType.CHAT_ACK).build(), session));
        verify(session, never()).tryConsumeCredits();
    }
}
