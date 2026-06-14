package org.hormigas.ws.infrastructure.cache.redis;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;

import java.time.Instant;

@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class RedisIdempotencyManager implements IdempotencyManager<Message> {

    @Inject
    ReactiveRedisDataSource redis;

    @Inject
    MessengerConfig config;


    private ReactiveValueCommands<String, Integer> valueCommand;
    private ReactiveKeyCommands<String> keyCommands;

    private int TTL_SECONDS;
    private static final int DUMMY_VALUE = 1;

    @PostConstruct
    void init() {
        valueCommand = redis.value(Integer.class);
        keyCommands = redis.key();
        TTL_SECONDS = config.idempotent().ttlSeconds();
    }

    private String messageKey(Message message) {
        return "receiver:"+message.getRecipientId()+":message:"+message.getMessageId();
    }

    @Override
    public Uni<StageResult<Message>> add(Message message) {
        log.debug("Adding message {}", message);
        return valueCommand.setex(messageKey(message), TTL_SECONDS, DUMMY_VALUE)
                .map(ignored -> StageResult.<Message>passed())
                .onFailure().invoke(ignored -> log.error("Error while adding message {}", message))
                .onFailure().recoverWithItem(StageResult.failed());
    }

    @Override
    public Uni<StageResult<Message>> remove(Message message) {
        log.debug("Removing message {}", message);
        return keyCommands.del(messageKey(message))
                .map(count -> count > 0 ? StageResult.<Message>passed() : StageResult.<Message>skipped())
                .onFailure().invoke(ignored -> log.error("Error while removing message {}", message))
                .onFailure().recoverWithItem(StageResult.failed());
    }

    @Override
    public Uni<Boolean> isInProgress(Message message) {
        log.debug("Checking message {}", message);
        return keyCommands.exists(messageKey(message)).onItem().invoke(exists -> {
           if (exists) log.debug("Message {} delivery duplication", messageKey(message));
        }).onFailure().invoke(ignored -> log.error("Error while checking progress message {}", message))
        .onFailure().recoverWithItem(Boolean.FALSE);
    }
}
