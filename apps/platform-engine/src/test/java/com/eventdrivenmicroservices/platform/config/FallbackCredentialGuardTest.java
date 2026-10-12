package com.eventdrivenmicroservices.platform.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class FallbackCredentialGuardTest {

    @Test
    void rejectsFallbackAdminPassword() {
        assertThrows(IllegalStateException.class, () -> new FallbackCredentialGuard("local-dev-password", "db-secret").verify());
    }

    @Test
    void rejectsFallbackDatabasePassword() {
        assertThrows(IllegalStateException.class, () -> new FallbackCredentialGuard("admin-secret", "local-dev-password").verify());
    }

    @Test
    void rejectsBlankPassword() {
        assertThrows(IllegalStateException.class, () -> new FallbackCredentialGuard(" ", "db-secret").verify());
    }

    @Test
    void acceptsExplicitSecrets() {
        assertDoesNotThrow(() -> new FallbackCredentialGuard("admin-secret", "db-secret").verify());
    }
}
