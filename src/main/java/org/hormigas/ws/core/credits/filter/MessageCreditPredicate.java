package org.hormigas.ws.core.credits.filter;

import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;

import java.util.function.Predicate;

public class MessageCreditPredicate implements Predicate<Message> {
    @Override
    public boolean test(Message message) {
        return message.getType() != MessageType.CHAT_ACK && message.getType() != MessageType.SIGNAL_ACK;
    }
}
