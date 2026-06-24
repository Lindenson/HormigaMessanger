package load;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * WebSocket + REST load simulation for the Messenger. Each virtual user is a fresh chat pair:
 * create the chat (REST) → open BOTH WS sockets (master + client, with Ory identity headers) →
 * the master streams {@code load.msgs} CHAT_IN messages while the client socket stays connected
 * (so the full inbound pipeline + persistence + live delivery + Tetris run under load).
 *
 * Run against a running prod app + infra:
 *   cd loadtest &amp;&amp; JAVA_HOME=/path/to/jdk-21 mvn gatling:test \
 *       -Dgatling.simulationClass=load.MessengerLoadSimulation \
 *       -Dload.users=200 -Dload.ramp=60 -Dload.msgs=30
 * Watch the server side via Prometheus /q/metrics (queue depth, outbox lag, GC, WS sessions).
 */
public class MessengerLoadSimulation extends Simulation {

    private static final int USERS = Integer.getInteger("load.users", 50);
    private static final int RAMP = Integer.getInteger("load.ramp", 30);
    private static final int MSGS = Integer.getInteger("load.msgs", 20);
    private static final String BASE = System.getProperty("load.baseUrl", "http://localhost:8080");
    private static final String WS_BASE = System.getProperty("load.wsUrl", "ws://localhost:8080");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE)
            .wsBaseUrl(WS_BASE)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json");

    private static final String CHAT_IN =
            "{\"type\":\"CHAT_IN\",\"senderId\":\"#{mId}\",\"recipientId\":\"#{cId}\"," +
            "\"conversationId\":\"#{conv}\",\"messageId\":\"#{mid}\",\"senderTimestamp\":#{now}," +
            "\"senderTimezone\":\"UTC\",\"payload\":{\"kind\":\"text\",\"body\":\"load\"}}";

    private final ScenarioBuilder scn = scenario("messenger-chat-pair")
            .exec(session -> {
                String uid = UUID.randomUUID().toString();
                return session
                        .set("cId", "lc-" + uid)
                        .set("mId", "lm-" + uid)
                        .set("now", System.currentTimeMillis());
            })
            .exec(http("create-chat").post("/api/chats")
                    .header("X-User-Id", "#{mId}").header("X-User", "M")
                    .header("X-Role", "MASTER").header("X-User-Email", "#{mId}@load")
                    .body(StringBody("{\"clientId\":\"#{cId}\",\"masterId\":\"#{mId}\",\"metadata\":{}}"))
                    .check(status().is(201), jsonPath("$.id").saveAs("conv")))
            .exec(ws("connect-master").wsName("master").connect("/ws")
                    .header("X-User-Id", "#{mId}").header("X-User", "M")
                    .header("X-Role", "MASTER").header("X-User-Email", "#{mId}@load"))
            .exec(ws("connect-client").wsName("client").connect("/ws")
                    .header("X-User-Id", "#{cId}").header("X-User", "C")
                    .header("X-Role", "CLIENT").header("X-User-Email", "#{cId}@load"))
            .pause(Duration.ofMillis(1200)) // both sessions register
            .repeat(MSGS).on(
                    exec(session -> session.set("mid", UUID.randomUUID().toString()))
                            .exec(ws("send-chat").wsName("master").sendText(CHAT_IN))
                            .pause(Duration.ofMillis(200))
            )
            .exec(ws("close-client").wsName("client").close())
            .exec(ws("close-master").wsName("master").close());

    {
        setUp(scn.injectOpen(rampUsers(USERS).during(Duration.ofSeconds(RAMP))))
                .protocols(httpProtocol);
    }
}
