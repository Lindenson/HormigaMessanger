package org.hormigas.ws.core.feedback.provider.inout;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import org.hormigas.ws.core.feedback.events.IncomingHealthEvent;
import org.hormigas.ws.core.feedback.events.OutgoingHealthEvent;
import org.hormigas.ws.core.feedback.provider.InEventProvider;
import org.hormigas.ws.core.feedback.provider.OutEventProvider;

@ApplicationScoped
public class FeedbackEventsProvider implements InEventProvider<IncomingHealthEvent>, OutEventProvider<OutgoingHealthEvent> {

    @Inject
    Event<OutgoingHealthEvent> eventBusOutgoing;
    @Inject
    Event<IncomingHealthEvent> eventBusIncoming;

    public void fireOut(OutgoingHealthEvent event) {
        eventBusOutgoing.fire(event);
    }

    public void fireIn(IncomingHealthEvent event) {
        eventBusIncoming.fire(event);
    }
}
