package hormiga;

import com.intuit.karate.junit5.Karate;

/**
 * Runs the Messenger REST acceptance suite (Karate OSS 1.4.1). WebSocket scenarios are NOT here —
 * they live in the JDK-WebSocket JUnit suite (hormiga.ws.*WsTest), because Karate 2.x paywalls
 * WebSocket and 1.4.1 can't build a JS handler on this JDK.
 * Run: mvn test -Dkarate.env=dev   (app + infra must be up — see docker-compose.yml).
 */
class MessengerRunner {

    @Karate.Test
    Karate all() {
        // ~@ws excludes the (now-retired) in-feature WebSocket scenarios; ~@wip excludes spec-only ones.
        return Karate.run("classpath:karate").tags("~@wip", "~@ws").relativeTo(getClass());
    }
}
