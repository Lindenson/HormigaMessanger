package org.hormigas.ws.infrastructure.cache.redis;


import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.presence.OnlineClient;
import org.hormigas.ws.ports.presence.PresenceManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
@Startup
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class RedisPresenceManager implements PresenceManager {

    private final static int BATCH_SIZE = 1000;

    private static final String SCRIPT = "local key = ARGV[1]\n" +
            "local ts = tonumber(ARGV[2])\n" +
            "local val = redis.call('GET', key)\n" +
            "if not val then return 0 end\n" +
            "local lastColon = val:match(\".*():\") -- возвращает позицию последнего ':'\n" +
            "if lastColon then\n" +
            "    local storedTs = tonumber(val:sub(lastColon + 1))\n" +
            "    if storedTs and storedTs <= ts then\n" +
            "        return redis.call('DEL', key)\n" +
            "    end\n" +
            "end\n" +
            "return 0\n";

    @Inject
    ReactiveRedisDataSource redis;

    @Inject
    RedisAPI lowLevelClient;

    private ReactiveValueCommands<String, String> valueCommand;
    private ReactiveKeyCommands<String> keyCommands;

    @PostConstruct
    void init() {
        valueCommand = redis.value(String.class);
        keyCommands = redis.key();
        //toDO
        cleanupAllClients().await().indefinitely();
    }


    private String clientKey(String clientId) {
        return "client:" + clientId;
    }

    private String clientValue(String name, long timestamp) {
        return "name:" + name + ":timestamp:" + timestamp;
    }

    @Override
    public Uni<Void> add(String id, String name, long timestamp) {
        return valueCommand.set(clientKey(id), clientValue(name, timestamp))
                .onFailure().invoke(throwable -> {
                    log.error("Error adding user {}", id, throwable);
                }).onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    public Uni<Void> remove(String id, long timestamp) {
        if (id == null) return Uni.createFrom().voidItem();

        List<String> args = List.of(SCRIPT, "0", clientKey(id), String.valueOf(timestamp));
        return lowLevelClient.eval(args).onItem()
                .invoke(it -> log.debug("Removed {} clients", it.toString()))
                .onFailure().invoke(er -> log.error("Error removing user {}", id, er))
                .onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    @Override
    public Uni<List<OnlineClient>> getAll() {
        KeyScanArgs args = new KeyScanArgs().match("client:*").count(100);

        return keyCommands.scan(args)
                .toMulti()
                .filter(reactiveKey -> reactiveKey != null && !reactiveKey.isEmpty())
                .group().intoLists().of(BATCH_SIZE)
                .onItem().transformToUni(keys ->
                        lowLevelClient.mget(keys)
                                .onItem().transform(mapResponse(keys))
                                .onFailure().invoke(er -> log.error("mget failed for keys {}: {}", keys, er.toString()))
                                .onFailure().recoverWithItem(List.of())
                )
                .concatenate().toUni()
                .replaceIfNullWith(List::of)
                .onFailure().invoke(er -> log.error("Failed to scan presence keys: {}", er.toString(), er))
                .onFailure().recoverWithItem(List.of());
    }

    private Function<Response, List<OnlineClient>> mapResponse(List<String> batchKeys) {
        return response -> {
            List<OnlineClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < batchKeys.size(); i++) {
                    String val = (response.get(i) != null) ? response.get(i).toString() : null;
                    var client = parseClientData(batchKeys.get(i), val);
                    if (client != null) clients.add(client);
                }
            } catch (Exception e) {
                log.error("Error parsing mget response for keys {} : {}", batchKeys, e.toString(), e);
                return List.of();
            }
            return clients;
        };
    }


    private OnlineClient parseClientData(String key, String value) {
        if (value == null) {
            return new OnlineClient(key, null, 0);
        }
        try {
            String[] parts = key.split(":");
            String id = parts.length == 2? parts[1] : key;
            parts = value.split(":");
            if (parts.length == 4) {
                String name = parts[1];
                long timestamp = Long.parseLong(parts[3]);
                return new OnlineClient(id, name, timestamp);
            }
        } catch (NumberFormatException e) {
            log.error("Error parsing value {}", value, e);
        }
        return null;
    }

    private Uni<Void> cleanupAllClients() {
        KeyScanArgs args = new KeyScanArgs().match("client:*").count(500);

        return keyCommands.scan(args)
                .toMulti()
                .filter(keys -> keys != null && !keys.isEmpty())
                .onItem().transformToUni(keys ->
                        keyCommands.del(keys)
                )
                .concatenate()
                .collect().asList()
                .replaceWithVoid();
    }
}
