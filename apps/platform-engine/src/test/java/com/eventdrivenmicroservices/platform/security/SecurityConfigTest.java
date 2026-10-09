package com.eventdrivenmicroservices.platform.security;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityConfigTest {

    private static final String SECRET = "test-secret-for-security-config";
    private final SecurityConfig securityConfig = new SecurityConfig();

    @Test
    void validatesHmacSignature() throws Exception {
        assertTrue(securityConfig.validateJwtToken(token(SECRET), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET), "wrong-secret"));
        assertFalse(securityConfig.validateJwtToken("not-a-jwt", SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET), null));
    }

    @Test
    void deployedProfilesRequireJwtSecret() {
        ReflectionTestUtils.setField(securityConfig, "activeProfiles", "dev,k8s");
        assertThrows(IllegalStateException.class, securityConfig::validateJwtConfiguration);

        ReflectionTestUtils.setField(securityConfig, "jwtSecretKey", SECRET);
        assertDoesNotThrow(securityConfig::validateJwtConfiguration);
    }

    @Test
    void localProfileAllowsJwtSecretToBeUnset() {
        ReflectionTestUtils.setField(securityConfig, "activeProfiles", "dev");
        assertDoesNotThrow(securityConfig::validateJwtConfiguration);
    }

    private String token(String secret) throws Exception {
        String headerAndPayload = encode("{\"alg\":\"HS256\"}") + "." + encode("{\"sub\":\"test\"}");
        Mac hmac = Mac.getInstance("HmacSHA256");
        hmac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String signature = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(hmac.doFinal(headerAndPayload.getBytes(StandardCharsets.UTF_8)));
        return headerAndPayload + "." + signature;
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}