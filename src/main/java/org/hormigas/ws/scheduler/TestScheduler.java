package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.generator.IdGenerator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
@ApplicationScoped
@IfBuildProfile("imitation")
public class TestScheduler {

    static final List<Clients> clients;

    static {
        clients = List.of(
                Clients.builder().clientId("cf72f28a-77c9-4eed-b591-beb612ec8307")
                        .message("Hello Vlad from database - ").build(),
                Clients.builder().clientId("c71e006c-5d07-4787-8054-2eb8feb8deb9")
                        .message("Hello Den from database - ").build()
        );
    }

    @Inject
    OutboxManager<Message> outboxManager;

    @Inject
    IdGenerator idGenerator;

    private static final AtomicInteger counter = new AtomicInteger(0);

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public Uni<Void> insertRandomClientMessage() {
        return Multi.createFrom().range(0, 500).onItem().transformToUniAndConcatenate(it -> {
            Message msg = Message.builder()
                    .messageId(idGenerator.generateId())
                    .sessionId(idGenerator.generateId())
                    .recipientId(clients.get(counter.get() % 2).clientId)
                    .serverTimestamp(System.currentTimeMillis())
                    .senderId("server")
                    .senderTimestamp(System.currentTimeMillis())
                    .type(MessageType.CHAT_OUT)
                    .payload(new Message.Payload("text", "Hello-" + counter.incrementAndGet()))
                    .build();

            return outboxManager.save(msg).replaceWithVoid();
        }).collect().asList().replaceWithVoid();
    }

    @Builder
    static class Clients {
        String clientId;
        String message;
    }
}
