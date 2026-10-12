package com.eventdrivenmicroservices.platform.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Refuses to start a deployed (k8s) instance that still uses the local development fallback credentials. */
@Component
@Profile("k8s")
public class FallbackCredentialGuard {

    static final String FALLBACK = "local-dev-password";

    private final String adminPassword;
    private final String databasePassword;

    public FallbackCredentialGuard(
            @Value("${spring.security.user.password:}") String adminPassword,
            @Value("${eventdrivenmicroservices.database.local.password:}") String databasePassword) {
        this.adminPassword = adminPassword;
        this.databasePassword = databasePassword;
    }

    @PostConstruct
    void verify() {
        check("SPRING_SECURITY_USER_PASSWORD", adminPassword);
        check("EVENTDRIVENMICROSERVICES_DATABASE_LOCAL_PASSWORD", databasePassword);
    }

    private static void check(String name, String value) {
        if (value == null || value.isBlank() || FALLBACK.equals(value)) {
            throw new IllegalStateException(name + " must be provided explicitly; the local development fallback is not allowed in the k8s profile");
        }
    }
}
