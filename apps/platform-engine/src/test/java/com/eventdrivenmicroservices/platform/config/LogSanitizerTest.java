package com.eventdrivenmicroservices.platform.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogSanitizerTest {

    @Test
    void stripsLineBreaksAndControlCharacters() {
        assertEquals("a_b_c_d", LogSanitizer.safe("a\nb\rc\td"));
        assertFalse(LogSanitizer.safe("forged\r\n[ERROR] injected").contains("\n"));
    }

    @Test
    void truncatesLongValuesAndHandlesNull() {
        String result = LogSanitizer.safe("x".repeat(500));
        assertTrue(result.length() <= 131);
        assertTrue(result.endsWith("..."));
        assertEquals("null", LogSanitizer.safe(null));
    }
}
