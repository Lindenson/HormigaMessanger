package hormiga;

import com.intuit.karate.junit5.Karate;

/**
 * Runs the Messenger E2E Karate suite (ATDD spec per use case).
 * Run: mvn test -Dkarate.env=dev   (app + infra must be up — see docker-compose.yml).
 */
class MessengerRunner {

    @Karate.Test
    Karate all() {
        // Default run = implemented scenarios only (green baseline). The @wip scenarios are the
        // ATDD spec for not-yet-built use cases; run them with -Dkarate.options="--tags @wip".
        return Karate.run("classpath:karate").tags("~@wip").relativeTo(getClass());
    }
}
