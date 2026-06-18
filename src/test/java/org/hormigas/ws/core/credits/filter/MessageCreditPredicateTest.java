package org.hormigas.ws.core.credits.filter;

import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("MessageCreditPredicate — only ACKs bypass credit accounting")
class MessageCreditPredicateTest {

    private final MessageCreditPredicate predicate = new MessageCreditPredicate();

    @ParameterizedTest
    @CsvSource({
            "CHAT_IN,    true",
            "SIGNAL_IN,  true",
            "READ_IN,    true",
            "CHAT_ACK,   false",
            "SIGNAL_ACK, false"
    })
    @DisplayName("non-ACK messages are credited; CHAT_ACK / SIGNAL_ACK are exempt")
    void chargesAllButAcks(MessageType type, boolean charged) {
        Message msg = Message.builder().type(type).build();
        assertEquals(charged, predicate.test(msg));
    }
}
