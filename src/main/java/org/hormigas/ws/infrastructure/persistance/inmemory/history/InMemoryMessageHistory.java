package org.hormigas.ws.infrastructure.persistance.inmemory.history;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.domain.message.Message;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "memory")
public class InMemoryMessageHistory implements History<Message> {

    private static final int MAX_MESSAGES_PER_CLIENT = 1000;
    private static final long MESSAGE_TTL_MILLIS = 60 * 60 * 1000;

    private final ConcurrentHashMap<String, Deque<MessageRecord>> receivedStorage = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<MessageRecord>> sentStorage = new ConcurrentHashMap<>();

    @Override
    public Uni<List<Message>> getByRecipientId(String clientId) {
        return Uni.createFrom().item(getMessagesFromDeque(receivedStorage.get(clientId)));
    }

    @Override
    public Uni<List<Message>> getBySenderId(String clientId) {
        return Uni.createFrom().item(getMessagesFromDeque(sentStorage.get(clientId)));
    }

    @Override
    public Uni<List<Message>> getAllBySenderId(String clientId) {
        Uni<List<Message>> received = getByRecipientId(clientId);
        Uni<List<Message>> sent = getBySenderId(clientId);

        return Uni.combine().all().unis(received, sent)
                .asTuple()
                .onItem().transform(tuple -> {
                    List<Message> allMessages = new ArrayList<>();
                    allMessages.addAll(tuple.getItem1());
                    allMessages.addAll(tuple.getItem2());
                    return allMessages;
                });
    }


    private List<Message> getMessagesFromDeque(Deque<MessageRecord> deque) {
        if (deque == null) return List.of();
        cleanupClientHistory(deque);
        return deque.stream().map(MessageRecord::message).toList();
    }


    @Override
    public void addBySenderId(String clientId, Message message) {
        if (message.getSenderId().equals("server")) return;

        receivedStorage.computeIfAbsent(clientId, k -> new ConcurrentLinkedDeque<>());
        var receivedDeque = receivedStorage.get(clientId);
        receivedDeque.addLast(new MessageRecord(message, Instant.now()));
        trimDeque(receivedDeque);

        String senderId = message.getSenderId();
        sentStorage.computeIfAbsent(senderId, k -> new ConcurrentLinkedDeque<>());
        var sentDeque = sentStorage.get(senderId);
        sentDeque.addLast(new MessageRecord(message, Instant.now()));
        trimDeque(sentDeque);
    }

    public void cleanupAll() {
        receivedStorage.forEach((id, deque) -> cleanupClientHistory(deque));
        sentStorage.forEach((id, deque) -> cleanupClientHistory(deque));
    }

    private void trimDeque(Deque<MessageRecord> deque) {
        while (deque.size() > MAX_MESSAGES_PER_CLIENT) {
            deque.pollFirst();
        }
    }

    private void cleanupClientHistory(Deque<MessageRecord> deque) {
        long cutoff = System.currentTimeMillis() - MESSAGE_TTL_MILLIS;
        deque.removeIf(record -> record.timestamp().toEpochMilli() < cutoff);
    }

    private record MessageRecord(Message message, Instant timestamp) {
    }
}
