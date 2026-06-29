package load;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Wave/topology stress: a large <b>online pool</b> with a small, <b>rotating</b> set of <b>high-rate</b>
 * senders, in load waves — the "3000 msg/s spread over 30 talkers, 1200 online" scenario.
 *
 * <ul>
 *   <li><b>{@code load.pool}</b> clients stay <b>online</b> the whole run (hold a WS socket, receive).</li>
 *   <li>A rotating set of <b>active talkers</b> each fire {@code ~1000/ratePerMsg} msg/s into a chat with a
 *       <b>random pool peer</b> (all-to-all coverage via rotation), then leave so a fresh talker takes the
 *       slot. At full load {@code load.activeFull} talkers × ~100/s ≈ the target aggregate.</li>
 *   <li><b>Three waves</b> over {@code 3 × load.waveSec}: 50% → 100% → 75% of active-talker concurrency.</li>
 * </ul>
 *
 * Sends are rate-paced (not ack-gated) so the offered rate is actually delivered; server-side truth
 * (persisted rows, drops, GC, memory) is captured out-of-band by the sampler. Recipients are online,
 * so every message is also delivered live.
 *
 * <pre>
 *   cd loadtest &amp;&amp; JAVA_HOME=/path/to/jdk-21 mvn gatling:test \
 *       -Dgatling.simulationClass=load.MessengerWaveSimulation \
 *       -Dload.pool=1170 -Dload.activeFull=30 -Dload.ratePerMsgMs=10 -Dload.waveSec=140 -Dload.burst=400
 * </pre>
 */
public class MessengerWaveSimulation extends Simulation {

    private static final int POOL = Integer.getInteger("load.pool", 1170);          // online receivers
    private static final int FULL = Integer.getInteger("load.activeFull", 30);      // concurrent talkers at 100%
    private static final int RATE_MS = Integer.getInteger("load.ratePerMsgMs", 10); // 10ms → ~100 msg/s per talker
    private static final int WAVE = Integer.getInteger("load.waveSec", 140);        // per-wave seconds (×3 ≈ 7 min)
    private static final int BURST = Integer.getInteger("load.burst", 400);         // msgs per talker bout (rotation)
    private static final String BASE = System.getProperty("load.baseUrl", "http://localhost:8080");
    private static final String WS_BASE = System.getProperty("load.wsUrl", "ws://localhost:8080");

    private static final int HALF = Math.max(1, FULL / 2);
    private static final int THREE_Q = Math.max(1, (FULL * 3) / 4);
    private static final int TOTAL = WAVE * 3;

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE).wsBaseUrl(WS_BASE)
            .contentTypeHeader("application/json").acceptHeader("application/json");

    private static final String[] BODIES = {
            "Здравствуйте! Когда удобно?", "Цена окончательная?", "Могу подъехать завтра",
            "Отправил фото, посмотрите", "Спасибо, договорились", "А гарантия есть?",
            "Давайте созвонимся", "Жду по адресу", "Готово, принимайте", "Уточните детали пожалуйста"
    };

    // Unique pool ids u-0..u-(POOL-1): one per online holder (queue feeder — each taken once).
    private static final Iterator<Map<String, Object>> poolFeeder = poolIds();

    private static Iterator<Map<String, Object>> poolIds() {
        List<Map<String, Object>> ids = new ArrayList<>(POOL);
        for (int i = 0; i < POOL; i++) {
            Map<String, Object> m = new HashMap<>();
            m.put("poolId", "u-" + i);
            ids.add(m);
        }
        return ids.iterator();
    }

    private static final Iterator<Map<String, Object>> msgFeeder = msgGen();

    private static Iterator<Map<String, Object>> msgGen() {
        return java.util.stream.Stream.generate(() -> {
            Map<String, Object> m = new HashMap<>();
            m.put("body", BODIES[ThreadLocalRandom.current().nextInt(BODIES.length)]);
            m.put("mid", UUID.randomUUID().toString());
            m.put("now", System.currentTimeMillis());
            return m;
        }).iterator();
    }

    private static final String CHAT_IN =
            "{\"type\":\"CHAT_IN\",\"senderId\":\"#{mId}\",\"recipientId\":\"#{peer}\","
                    + "\"conversationId\":\"#{conv}\",\"messageId\":\"#{mid}\",\"senderTimestamp\":#{now},"
                    + "\"senderTimezone\":\"UTC\",\"payload\":{\"kind\":\"text\",\"body\":\"#{body}\"}}";

    // 1,200 online: each connects as a pool client and holds the socket (receives live delivery) for the run.
    private final ScenarioBuilder online = scenario("online-pool")
            .feed(poolFeeder)
            .exec(ws("pool-connect").wsName("c").connect("/ws")
                    .header("X-User-Id", "#{poolId}").header("X-User", "C")
                    .header("X-Role", "CLIENT").header("X-User-Email", "#{poolId}@load"))
            .pause(Duration.ofSeconds(TOTAL + 90)) // never finishes during the run → stays online
            .exec(ws("pool-close").wsName("c").close());

    // A rotating high-rate talker: pick a random online peer, open a chat, fire ~100/s for a short bout, leave.
    private final ScenarioBuilder talker = scenario("active-talkers")
            .exec(session -> session
                    .set("mId", "t-" + UUID.randomUUID())
                    .set("peer", "u-" + ThreadLocalRandom.current().nextInt(POOL)))
            .exec(http("wave-create-chat").post("/api/chats")
                    .header("X-User-Id", "load-svc").header("X-User", "Load Service")
                    .header("X-Role", "SERVICE").header("X-User-Email", "svc@load")
                    .body(StringBody("{\"clientId\":\"#{peer}\",\"masterId\":\"#{mId}\",\"metadata\":{\"orderId\":\"#{mId}\"}}"))
                    .check(status().in(200, 201), jsonPath("$.id").saveAs("conv")))
            .exec(ws("wave-connect").wsName("m").connect("/ws")
                    .header("X-User-Id", "#{mId}").header("X-User", "M")
                    .header("X-Role", "MASTER").header("X-User-Email", "#{mId}@load"))
            .repeat(BURST).on(
                    feed(msgFeeder)
                            .exec(ws("wave-send").wsName("m").sendText(CHAT_IN))
                            .pace(Duration.ofMillis(RATE_MS)) // ~1000/RATE_MS msg/s per talker
            )
            .exec(ws("wave-close").wsName("m").close());

    {
        setUp(
                online.injectClosed(
                        rampConcurrentUsers(0).to(POOL).during(Duration.ofSeconds(30)),
                        constantConcurrentUsers(POOL).during(Duration.ofSeconds(TOTAL))),
                talker.injectClosed(
                        // wave 1 — 50%
                        rampConcurrentUsers(0).to(HALF).during(Duration.ofSeconds(10)),
                        constantConcurrentUsers(HALF).during(Duration.ofSeconds(WAVE - 10)),
                        // wave 2 — 100%
                        rampConcurrentUsers(HALF).to(FULL).during(Duration.ofSeconds(5)),
                        constantConcurrentUsers(FULL).during(Duration.ofSeconds(WAVE - 5)),
                        // wave 3 — 75%
                        constantConcurrentUsers(THREE_Q).during(Duration.ofSeconds(WAVE)))
        ).protocols(httpProtocol).maxDuration(Duration.ofSeconds(TOTAL + 90));
    }
}
