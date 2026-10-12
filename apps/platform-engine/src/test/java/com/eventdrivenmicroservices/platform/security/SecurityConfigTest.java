package com.eventdrivenmicroservices.platform.security;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void rejectsExpiredMissingExpiryAndNotYetValidTokens() throws Exception {
        long now = Instant.now().getEpochSecond();

        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256", "{\"sub\":\"a\",\"exp\":" + (now - 3600) + "}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256", "{\"sub\":\"a\"}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256", "{\"sub\":\"a\",\"exp\":\"later\"}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(
                token(SECRET, "HS256", "{\"sub\":\"a\",\"exp\":" + (now + 3600) + ",\"nbf\":" + (now + 3600) + "}"), SECRET));
    }

    @Test
    void toleratesSmallClockSkew() throws Exception {
        long now = Instant.now().getEpochSecond();

        assertTrue(securityConfig.validateJwtToken(token(SECRET, "HS256", "{\"exp\":" + (now - 5) + "}"), SECRET));
    }

    @Test
    void acceptsPreviousSecretsDuringRotationAndRejectsRetiredOnes() throws Exception {
        long exp = Instant.now().getEpochSecond() + 3600;
        String claims = "{\"sub\":\"sensor-1\",\"exp\":" + exp + "}";
        String oldToken = token("old-secret", "HS256", claims);
        String newToken = token(SECRET, "HS256", claims);
        ReflectionTestUtils.setField(securityConfig, "jwtSecretKey", SECRET);

        assertNull(securityConfig.authenticatedSubjectWithRotation(oldToken, Instant.now()));

        ReflectionTestUtils.setField(securityConfig, "jwtPreviousSecretKeys", "older-secret, old-secret");
        assertEquals("sensor-1", securityConfig.authenticatedSubjectWithRotation(oldToken, Instant.now()));
        assertEquals("sensor-1", securityConfig.authenticatedSubjectWithRotation(newToken, Instant.now()));
        assertNull(securityConfig.authenticatedSubjectWithRotation(token("unknown", "HS256", claims), Instant.now()));

        ReflectionTestUtils.setField(securityConfig, "jwtPreviousSecretKeys", null);
        assertNull(securityConfig.authenticatedSubjectWithRotation(oldToken, Instant.now()));
    }

    @Test
    void rejectsNonHs256AlgorithmsIncludingNone() throws Exception {
        String claims = validClaims();

        assertFalse(securityConfig.validateJwtToken(token(SECRET, "none", claims), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS512", claims), SECRET));
        assertFalse(securityConfig.validateJwtToken(encode("{\"alg\":\"none\"}") + "." + encode(claims) + ".", SECRET));
    }

    @Test
    void enforcesConfiguredIssuerAndAudience() throws Exception {
        ReflectionTestUtils.setField(securityConfig, "jwtIssuer", "edge-issuer");
        ReflectionTestUtils.setField(securityConfig, "jwtAudience", "platform-engine");
        long exp = Instant.now().getEpochSecond() + 3600;

        assertTrue(securityConfig.validateJwtToken(token(SECRET, "HS256",
                "{\"iss\":\"edge-issuer\",\"aud\":\"platform-engine\",\"exp\":" + exp + "}"), SECRET));
        assertTrue(securityConfig.validateJwtToken(token(SECRET, "HS256",
                "{\"iss\":\"edge-issuer\",\"aud\":[\"other\",\"platform-engine\"],\"exp\":" + exp + "}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256",
                "{\"iss\":\"someone-else\",\"aud\":\"platform-engine\",\"exp\":" + exp + "}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256",
                "{\"iss\":\"edge-issuer\",\"aud\":\"other\",\"exp\":" + exp + "}"), SECRET));
        assertFalse(securityConfig.validateJwtToken(token(SECRET, "HS256", "{\"exp\":" + exp + "}"), SECRET));
    }

    @Test
    void usesSubjectAsPrincipalWithFallback() throws Exception {
        long exp = Instant.now().getEpochSecond() + 3600;

        assertEquals("sensor-7", securityConfig.authenticatedSubject(
                token(SECRET, "HS256", "{\"sub\":\"sensor-7\",\"exp\":" + exp + "}"), SECRET, Instant.now()));
        assertEquals("eventdrivenmicroservices-edge-node", securityConfig.authenticatedSubject(
                token(SECRET, "HS256", "{\"exp\":" + exp + "}"), SECRET, Instant.now()));
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
        return token(secret, "HS256", validClaims());
    }

    private String validClaims() {
        return "{\"sub\":\"test\",\"exp\":" + (Instant.now().getEpochSecond() + 3600) + "}";
    }

    private String token(String secret, String alg, String claims) throws Exception {
        String headerAndPayload = encode("{\"alg\":\"" + alg + "\"}") + "." + encode(claims);
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