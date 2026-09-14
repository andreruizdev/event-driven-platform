package com.eventdrivenmicroservices.platform;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(packages = "com.eventdrivenmicroservices.platform..")
public class ArchitectureTest {

    // HARD GUARDRAIL: Rule #2 in AGENTS.md
    // "Non-Blocking Java Backend: Any Spring Boot endpoints MUST follow the reactive non-blocking pattern."
    @ArchTest
    static final ArchRule no_thread_sleep_allowed =
        noClasses().that().resideInAPackage("com.eventdrivenmicroservices.platform..")
            .should().callMethod(Thread.class, "sleep", long.class)
            .because("We are a purely Reactive application (WebFlux). Thread.sleep violates non-blocking event loop processing.");
}
