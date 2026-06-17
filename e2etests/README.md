# HormigaMessanger — E2E Karate harness (ATDD)

Acceptance suite that follows the use-case spec
(`HormigasAdmin/knowledge/concepts/messenger-use-cases.md`). Scenarios are named `UC-U##`/`UC-H##`.
Not-yet-implemented use cases are tagged **`@wip`** (the spec, red until built) and excluded from
the default run; drop the tag as each goes green.

## Run locally

```bash
# 1. infra
docker compose -f e2etests/docker-compose.yml up -d

# 2. app (built at release 25; point it at local infra)
JAVA_HOME=/opt/jdk-25.0.2 ./mvnw -q clean package -DskipTests
DB_HOST=localhost REDIS_HOST=localhost \
  java -Djava.util.logging.manager=org.jboss.logmanager.LogManager \
       -jar target/quarkus-app/quarkus-run.jar &

# 3. tests (Karate runs on JDK 21)
cd e2etests
JAVA_HOME=/opt/jdk-21.0.2 mvn -q test -Dkarate.env=dev            # green baseline (implemented)
JAVA_HOME=/opt/jdk-21.0.2 mvn -q test -Dkarate.options="--tags @wip"   # the pending spec
```

Staging: `-Dkarate.env=staging` (baseUrl → 91.99.6.25:8080).

## Layout
- `karate/auth/` — UC-U60/U61 identity & authz (implemented)
- `karate/chat/` — UC-U01/U02/U03/H02 chat lifecycle (`@wip` → drives M-2)
- `karate/messaging/` — UC-U10..U16 send/deliver/ack (`@wip` → drives M-2/M-3)
