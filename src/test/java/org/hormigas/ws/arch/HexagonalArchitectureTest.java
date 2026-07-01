package org.hormigas.ws.arch;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.websockets.next.WebSocket;
import jakarta.ws.rs.Path;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.hormigas.ws.domain.conversation.Conversation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static com.tngtech.archunit.lang.conditions.ArchConditions.callMethod;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Architecture ratchet — encodes the canonical style specified in {@code docs/ARCHITECTURE.md},
 * reverse-engineered from the engine baseline (commit {@code 04a2f70a}). These rules enforce strict
 * conformance to that architecture; they may only be tightened, never loosened or ignored. Build-time.
 *
 * <p>Accepted compromises: {@code com.fasterxml} (Jackson) on the wire {@code Message}; CDI scope
 * annotations ({@code jakarta.enterprise}/{@code jakarta.inject}) and {@code io.smallrye.mutiny} in
 * domain/ports (baseline-faithful — the original domain validator is a CDI bean).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HexagonalArchitectureTest {

    private JavaClasses classes;

    @BeforeAll
    void importClasses() throws Exception {
        java.nio.file.Path classesDir = java.nio.file.Path.of(Conversation.class
                .getProtectionDomain().getCodeSource().getLocation().toURI());
        classes = new ClassFileImporter().importPath(classesDir.resolve("org/hormigas/ws"));
        assertFalse(classes.isEmpty(), "ArchUnit imported no classes");
    }

    // ─────────────────────────── Layering (dependency direction) ───────────────────────────

    @Test
    void domain_is_framework_free_and_independent_of_infrastructure() {
        // Faithful to the baseline: domain forbids frameworks + infrastructure. (It deliberately does
        // NOT forbid core/ports here — the baseline ClientSession→core.credits.Credits coupling exists;
        // that minor smell is documented in ARCHITECTURE.md, not enforced, to keep the rule an honest
        // encoding of the existing architecture rather than stricter than it.)
        ArchRule rule = noClasses()
                .that().resideInAPackage("org.hormigas.ws.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.hormigas.ws.infrastructure..",
                        "io.quarkus..", "io.vertx..", "jakarta.ws..", "jakarta.persistence..",
                        "org.apache.kafka..", "io.minio..", "com.github.f4b6a3..")
                .because("the domain is the innermost layer — framework-free "
                        + "(CDI scope annotations + Jackson on Message are the only accepted compromises)");
        rule.check(classes);
    }

    @Test
    void core_does_not_depend_on_infrastructure() {
        noClasses().that().resideInAPackage("org.hormigas.ws.core..")
                .should().dependOnClassesThat().resideInAPackage("org.hormigas.ws.infrastructure..")
                .because("core orchestrates via ports only, never infrastructure adapters")
                .check(classes);
    }

    @Test
    void core_is_free_of_transport_and_persistence_tech() {
        noClasses().that().resideInAPackage("org.hormigas.ws.core..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "io.vertx..", "jakarta.ws..", "jakarta.persistence..",
                        "org.apache.kafka..", "io.minio..", "io.lettuce..")
                .because("core holds business/engine logic — DB, HTTP, Kafka, MinIO live behind ports")
                .check(classes);
    }

    @Test
    void ports_do_not_depend_on_core_or_infrastructure() {
        noClasses().that().resideInAPackage("org.hormigas.ws.ports..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.hormigas.ws.core..", "org.hormigas.ws.infrastructure..")
                .because("ports are contracts — they must not know their callers or adapters")
                .check(classes);
    }

    // ─────────────────────────── Naming (docs/ARCHITECTURE.md §3) ───────────────────────────

    @Test
    void core_classes_are_not_named_Service() {
        noClasses().that().resideInAPackage("org.hormigas.ws.core..")
                .should().haveSimpleNameEndingWith("Service")
                .because("core is named by what a thing IS (poller, watermark, router) — not the generic 'Service' suffix")
                .check(classes);
    }

    @Test
    void ports_are_interfaces() {
        classes().that().resideInAPackage("org.hormigas.ws.ports..").and().areTopLevelClasses()
                .should().beInterfaces()
                .because("a port is a contract")
                .check(classes);
    }

    // ─────────────────────────── Ports / adapters / repositories (§2) ───────────────────────────

    @Test
    void repository_is_an_infrastructure_only_name() {
        classes().that().haveSimpleNameEndingWith("Repository")
                .should().resideInAPackage("org.hormigas.ws.infrastructure..")
                .because("'Repository' is implementation-internal; it must never leak into ports or core")
                .check(classes);
    }

    @Test
    void core_and_ports_do_not_depend_on_repositories() {
        noClasses().that().resideInAnyPackage("org.hormigas.ws.core..", "org.hormigas.ws.ports..")
                .should().dependOnClassesThat().haveSimpleNameEndingWith("Repository")
                .because("core talks to a port (role); the backing repository is invisible to it")
                .check(classes);
    }

    @Test
    void adapters_reside_in_infrastructure() {
        classes().that().haveSimpleNameEndingWith("Adapter")
                .should().resideInAPackage("org.hormigas.ws.infrastructure..")
                .because("adapters implement ports and live in infrastructure")
                .check(classes);
    }

    @Test
    void mappers_reside_in_infrastructure() {
        classes().that().haveSimpleNameEndingWith("Mapper")
                .should().resideInAPackage("org.hormigas.ws.infrastructure..")
                .because("domain↔DTO mapping is an infrastructure concern")
                .check(classes);
    }

    // ─────────────────────────── Transport annotations live only in their adapter slice (§4) ───────────────────────────

    @Test
    void rest_resources_reside_in_rest_infrastructure() {
        classes().that().areAnnotatedWith(Path.class)
                .should().resideInAPackage("org.hormigas.ws.infrastructure.rest..")
                .because("JAX-RS is a transport adapter")
                .check(classes);
        classes().that().haveSimpleNameEndingWith("Resource")
                .should().resideInAPackage("org.hormigas.ws.infrastructure.rest..")
                .check(classes);
    }

    @Test
    void websocket_endpoints_reside_in_websocket_infrastructure() {
        classes().that().areAnnotatedWith(WebSocket.class)
                .should().resideInAPackage("org.hormigas.ws.infrastructure.websocket..")
                .because("the WebSocket transport is an infrastructure adapter")
                .check(classes);
    }

    @Test
    void kafka_consumers_reside_in_messaging_infrastructure() {
        // @Incoming is a method-level annotation → match methods, not types.
        methods().that().areAnnotatedWith(Incoming.class)
                .should().beDeclaredInClassesThat().resideInAPackage("org.hormigas.ws.infrastructure.messaging..")
                .because("Kafka is a driving adapter — consumers live in infrastructure.messaging")
                .check(classes);
    }

    @Test
    void schedulers_reside_in_scheduler_package() {
        // @Scheduled is a method-level annotation → match methods, not types.
        methods().that().areAnnotatedWith(Scheduled.class)
                .should().beDeclaredInClassesThat().resideInAPackage("org.hormigas.ws.scheduler..")
                .because("@Scheduled entry points that drive core loops live in the top-level scheduler package")
                .check(classes);
    }

    // ─────────────────────────── Adapter purity (§4) ───────────────────────────

    @Test
    void websocket_ingress_is_pure_transport() {
        // Targets the @WebSocket ingress handler specifically — NOT the legit outbound adapters under
        // infrastructure.websocket (e.g. Deliverer implements the DeliveryChannel port, which is correct).
        noClasses().that().haveSimpleName("WebsocketService")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.hormigas.ws.core.conversation..", "org.hormigas.ws.core.message..",
                        "org.hormigas.ws.core.attachment..", "org.hormigas.ws.core.notify..",
                        "org.hormigas.ws.ports.message..", "org.hormigas.ws.ports.history..",
                        "org.hormigas.ws.ports.outbox..", "org.hormigas.ws.ports.deadletter..",
                        "org.hormigas.ws.ports.channel..", "org.hormigas.ws.ports.conversation..")
                .because("the WS ingress is pure transport: deserialize + filter + validate + publish; "
                        + "membership/recipient/read/system-ack are use-cases done downstream in the pipeline")
                .check(classes);
    }

    /** A concrete (non-interface, non-enum) class living in core — i.e. a core implementation. */
    private static final DescribedPredicate<JavaClass> CONCRETE_CORE_CLASS =
            new DescribedPredicate<>("a concrete class in core") {
                @Override
                public boolean test(JavaClass jc) {
                    return jc.getPackageName().startsWith("org.hormigas.ws.core")
                            && !jc.isInterface() && !jc.isEnum();
                }
            };

    @Test
    void rest_and_messaging_adapters_depend_on_abstractions_not_concrete_core() {
        // Driving adapters reach the core only through an abstraction: a core driving interface
        // (e.g. Chats/Uploads/Notices) or a driven port — never a concrete core use-case class.
        noClasses()
                .that().resideInAnyPackage("org.hormigas.ws.infrastructure.rest..",
                        "org.hormigas.ws.infrastructure.messaging..")
                .should().dependOnClassesThat(CONCRETE_CORE_CLASS)
                .because("infrastructure holds no business logic; it depends on interfaces (core driving "
                        + "interfaces or ports), not on concrete core implementations")
                .check(classes);
    }

    @Test
    void infrastructure_does_not_make_domain_decisions() {
        noClasses().that().resideInAPackage("org.hormigas.ws.infrastructure..")
                .should(callMethod(Conversation.class, "hasParticipant", String.class)
                        .or(callMethod(Conversation.class, "isBlocked")))
                .because("authorization/membership decisions belong in core/domain, not in an adapter")
                .check(classes);
    }

    // ─────────────────────────── The router is the single message pipeline ───────────────────────────

    @Test
    void message_delivery_flows_only_through_the_router() {
        // The router is the one crossroads and pipeline for every message: ingress publishes into it,
        // and client delivery happens ONLY inside the pipeline (DeliveryStage / ReadStage). No adapter
        // and no other core code may deliver a message to a client directly — it must route.
        // (Sole documented transport-level exception: WebsocketService.notifyOverloaded, the ingress-
        // reject signal fired when the router's own queue is full — it cannot use the saturated pipeline;
        // it writes the raw socket, not DeliveryChannel, so it is not caught here.)
        noClasses().that().resideOutsideOfPackage("org.hormigas.ws.core.router..")
                .should(callMethod(org.hormigas.ws.ports.channel.DeliveryChannel.class, "deliver", Object.class))
                .because("the router is the single pipeline for all messages — delivery goes through the "
                        + "pipeline stages, never straight from an adapter or other core code")
                .check(classes);
    }

    // ─────────────────────────── Validation (§5) ───────────────────────────

    @Test
    void domain_validators_reside_in_domain() {
        classes().that().implement(org.hormigas.ws.domain.validator.Validator.class)
                .should().resideInAPackage("org.hormigas.ws.domain..")
                .because("validation is a domain concern (the original MessageValidator lives in domain/validator)")
                .check(classes);
    }
}
