package org.hormigas.ws.core.credits.filter;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.credits.ChannelFilter;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.domain.message.Message;

import java.util.function.Predicate;

@Slf4j
public class InboundMessageFilter<W> implements ChannelFilter<Message, W> {

    Predicate<Message> creditPolicy = new MessageCreditPredicate();

    @Override
    public boolean filter(@Nullable Message message, @Nonnull ClientSession<W> clientSession) {
        if (message == null) {
            log.error("Message is null for a client {}", clientSession.getClientId());
            return false;
        }
        if (creditPolicy.test(message) && !clientSession.tryConsumeCredits()) {
            log.warn("Rate limit exceeded for client {}", clientSession.getClientId());
            return false;
        }
        return true;
    }
}
