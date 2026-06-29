package load;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Realistic conversation load for the Messenger — models how real master↔client chats actually behave,
 * not a synthetic throughput hammer. Each virtual user is a chat pair that:
 * <ol>
 *   <li>is provisioned by the platform (service-to-service {@code POST /api/chats}, D4),</li>
 *   <li>opens both WS sockets (master + client) with Ory identity headers,</li>
 *   <li>holds a <b>turn-taking conversation</b>: a side sends {@code CHAT_IN} and awaits its {@code SENT}
 *       ack, the peer <b>marks it read</b> ({@code READ_IN}), then replies — with <b>human think-time</b>
 *       between turns and varied message bodies,</li>
 *   <li>and for a configurable fraction, the client <b>disconnects and reconnects</b> mid-chat, then
 *       <b>syncs history</b> over REST — exercising offline-hold + poller re-delivery + reconnect sync.</li>
 * </ol>
 *
 * <p>Because every {@code CHAT_IN} awaits the server's {@code CHAT_ACK} (which is emitted only after the
 * DB commit), reported latency/KO are honest server-side persistence figures, and each user self-throttles
 * to the server's real ack rate. Think-time makes the offered load resemble live traffic.
 *
 * <pre>
 *   cd loadtest &amp;&amp; JAVA_HOME=/path/to/jdk-21 mvn gatling:test \
 *       -Dgatling.simulationClass=load.MessengerLoadSimulation \
 *       -Dload.users=500 -Dload.ramp=30 -Dload.duration=300 \
 *       -Dload.turns=20 -Dload.minThinkMs=1500 -Dload.maxThinkMs=7000 -Dload.reconnectPct=30
 * </pre>
 * {@code load.users} = concurrent chat <i>pairs</i> (so 500 pairs = 1000 live WS clients).
 */
public class MessengerLoadSimulation extends Simulation {

    private static final int USERS = Integer.getInteger("load.users", 500);       // chat pairs (×2 = WS clients)
    private static final int RAMP = Integer.getInteger("load.ramp", 30);
    private static final int DURATION = Integer.getInteger("load.duration", 300);
    private static final int TURNS = Integer.getInteger("load.turns", 20);        // master↔client exchanges per chat
    private static final int MIN_THINK = Integer.getInteger("load.minThinkMs", 1500);
    private static final int MAX_THINK = Integer.getInteger("load.maxThinkMs", 7000);
    private static final int RECONNECT_PCT = Integer.getInteger("load.reconnectPct", 30);
    private static final String BASE = System.getProperty("load.baseUrl", "http://localhost:8080");
    private static final String WS_BASE = System.getProperty("load.wsUrl", "ws://localhost:8080");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE)
            .wsBaseUrl(WS_BASE)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json");

    // Varied, realistic marketplace chat lines (a side picks one at random per turn).
    private static final String[] BODIES = {
            "Здравствуйте! Когда вам удобно?",
            "Добрый день, подскажите по срокам",
            "Цена окончательная или возможен торг?",
            "Могу подъехать завтра после обеда",
            "Отправил фото, посмотрите пожалуйста",
            "Спасибо, договорились",
            "А гарантия на работу есть?",
            "Давайте созвонимся, так быстрее",
            "Ок, жду подтверждения по адресу",
            "Готово, можете принимать"
    };

    private static final Iterator<Map<String, Object>> bodyFeeder = Stream.generate(() -> {
        Map<String, Object> m = new HashMap<>();
        m.put("body", BODIES[ThreadLocalRandom.current().nextInt(BODIES.length)]);
        m.put("mid", UUID.randomUUID().toString());
        m.put("now", System.currentTimeMillis());
        return m;
    }).iterator();

    private static String chatIn(String fromVar, String toVar) {
        return "{\"type\":\"CHAT_IN\",\"senderId\":\"#{" + fromVar + "}\",\"recipientId\":\"#{" + toVar + "}\","
                + "\"conversationId\":\"#{conv}\",\"messageId\":\"#{mid}\",\"senderTimestamp\":#{now},"
                + "\"senderTimezone\":\"UTC\",\"payload\":{\"kind\":\"text\",\"body\":\"#{body}\"}}";
    }

    private static String readIn(String fromVar) {
        return "{\"type\":\"READ_IN\",\"senderId\":\"#{" + fromVar + "}\",\"conversationId\":\"#{conv}\","
                + "\"messageId\":\"#{mid}\",\"senderTimestamp\":#{now},\"senderTimezone\":\"UTC\"}";
    }

    // One CHAT_IN that awaits its SENT ack (server-side persistence confirmation).
    private static io.gatling.javaapi.core.ChainBuilder sendAndAck(String wsName, String label, String from, String to) {
        return exec(feed(bodyFeeder))
                .exec(ws(label).wsName(wsName).sendText(chatIn(from, to))
                        .await(10).on(ws.checkTextMessage(label + "-ack")
                                .matching(jsonPath("$.type").is("CHAT_ACK"))
                                .check(jsonPath("$.type").is("CHAT_ACK"))));
    }

    private final ScenarioBuilder scn = scenario("messenger-realistic-conversation")
            .exec(session -> {
                String uid = UUID.randomUUID().toString();
                return session
                        .set("cId", "lc-" + uid)
                        .set("mId", "lm-" + uid)
                        .set("reconnect", ThreadLocalRandom.current().nextInt(100) < RECONNECT_PCT);
            })
            // platform provisions the chat (SERVICE), then both parties come online
            .exec(http("create-chat").post("/api/chats")
                    .header("X-User-Id", "load-svc").header("X-User", "Load Service")
                    .header("X-Role", "SERVICE").header("X-User-Email", "svc@load")
                    .body(StringBody("{\"clientId\":\"#{cId}\",\"masterId\":\"#{mId}\",\"metadata\":{\"orderId\":\"#{mId}\"}}"))
                    .check(status().is(201), jsonPath("$.id").saveAs("conv")))
            .exec(ws("connect-master").wsName("master").connect("/ws")
                    .header("X-User-Id", "#{mId}").header("X-User", "M")
                    .header("X-Role", "MASTER").header("X-User-Email", "#{mId}@load"))
            .exec(ws("connect-client").wsName("client").connect("/ws")
                    .header("X-User-Id", "#{cId}").header("X-User", "C")
                    .header("X-Role", "CLIENT").header("X-User-Email", "#{cId}@load"))
            .pause(Duration.ofMillis(1200)) // both sessions register presence
            // a real back-and-forth: master speaks → client reads → client replies → master reads
            .repeat(TURNS).on(
                    sendAndAck("master", "master-says", "mId", "cId")
                            .pause(Duration.ofMillis(MIN_THINK), Duration.ofMillis(MAX_THINK))
                            .exec(feed(bodyFeeder))
                            .exec(ws("client-reads").wsName("client").sendText(readIn("cId")))
                            .exec(sendAndAck("client", "client-replies", "cId", "mId"))
                            .pause(Duration.ofMillis(MIN_THINK), Duration.ofMillis(MAX_THINK))
                            .exec(feed(bodyFeeder))
                            .exec(ws("master-reads").wsName("master").sendText(readIn("mId")))
            )
            // a fraction background the app and come back: drop the client, return, sync history over REST
            .doIf(session -> session.getBoolean("reconnect")).then(
                    exec(ws("bg-close-client").wsName("client").close())
                            .pause(Duration.ofSeconds(2), Duration.ofSeconds(6))
                            .exec(ws("reconnect-client").wsName("client").connect("/ws")
                                    .header("X-User-Id", "#{cId}").header("X-User", "C")
                                    .header("X-Role", "CLIENT").header("X-User-Email", "#{cId}@load"))
                            .pause(Duration.ofMillis(800))
                            .exec(http("history-sync").get("/api/chats/#{conv}/messages?limit=50")
                                    .header("X-User-Id", "#{cId}").header("X-User", "C")
                                    .header("X-Role", "CLIENT").header("X-User-Email", "#{cId}@load")
                                    .check(status().is(200)))
            )
            .exec(ws("close-client").wsName("client").close())
            .exec(ws("close-master").wsName("master").close());

    {
        // Closed model: ramp to USERS concurrent pairs, then hold them live for the soak.
        setUp(scn.injectClosed(
                        rampConcurrentUsers(1).to(USERS).during(Duration.ofSeconds(RAMP)),
                        constantConcurrentUsers(USERS).during(Duration.ofSeconds(DURATION))))
                .protocols(httpProtocol)
                .maxDuration(Duration.ofSeconds(RAMP + DURATION + 30));
    }
}
