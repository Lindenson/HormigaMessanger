package org.hormigas.ws.arch;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Path;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Hexagonal-architecture ratchet (M-8 / #23) — keeps layering clean (Order is the reference);
 * regressions are caught at build time.
 *
 * <p>Allowed compromises (documented): {@code com.fasterxml} (Jackson) on the wire/domain
 * {@code Message}, and {@code io.smallrye.mutiny.Uni} in ports — accepted for a Quarkus-reactive
 * codebase. Uses an explicit importer (the @AnalyzeClasses engine imported nothing under the
 * Quarkus surefire classpath).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HexagonalArchitectureTest {

    private JavaClasses classes;

    @BeforeAll
    void importClasses() throws Exception {
        // Derive target/classes from a main class's code-source location — robust against the
        // Quarkus surefire CWD/classloader (its default classpath scan imports nothing here).
        Path classesDir = Path.of(org.hormigas.ws.domain.conversation.Conversation.class
                .getProtectionDomain().getCodeSource().getLocation().toURI());
        classes = new ClassFileImporter().importPath(classesDir.resolve("org/hormigas/ws"));
        assertFalse(classes.isEmpty(), "ArchUnit imported no classes from " + classesDir);
    }

    @Test
    void domain_is_framework_free() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("org.hormigas.ws.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "io.quarkus..", "jakarta.enterprise..", "io.vertx..",
                        "com.github.f4b6a3..", "org.hormigas.ws.infrastructure..")
                .because("the domain must stay framework-free (Order is the reference)");
        rule.check(classes);
    }

    @Test
    void core_does_not_depend_on_infrastructure() {
        noClasses()
                .that().resideInAPackage("org.hormigas.ws.core..")
                .should().dependOnClassesThat().resideInAPackage("org.hormigas.ws.infrastructure..")
                .because("the core orchestrates via ports only, never infrastructure adapters")
                .check(classes);
    }

    @Test
    void ports_do_not_depend_on_core_or_infrastructure() {
        noClasses()
                .that().resideInAPackage("org.hormigas.ws.ports..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.hormigas.ws.core..", "org.hormigas.ws.infrastructure..")
                .because("ports are contracts — they must not know their callers or adapters")
                .check(classes);
    }
}
