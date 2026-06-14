package org.hormigas.ws.core.presence.repository;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.presence.AsyncPresence;
import org.hormigas.ws.core.presence.Presence;
import org.hormigas.ws.ports.presence.PresenceManager;

@Slf4j
@ApplicationScoped
public class AsyncClientPresence implements AsyncPresence {

    Presence delegate;

    @Inject
    PresenceManager presenceManager;

    @PostConstruct
    void init() {
        delegate = new ClientPresence(presenceManager);
    }

    @Override
    public void add(String userId, String name, long timestamp) {
        delegate.add(userId, name, timestamp)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .subscribe().with(
                        ignored -> log.debug("Client {} added to presence", userId),
                        failure -> log.error("Failed to add client to presence", failure)
                );
    }

    @Override
    public void remove(String userId, long timestamp) {
        delegate.remove(userId, timestamp)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .subscribe().with(
                        ignored -> log.debug("Client {} removed to presence", userId),
                        failure -> log.error("Failed to remove client to presence", failure)
                );
    }
}
