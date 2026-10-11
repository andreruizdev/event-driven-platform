package com.eventdrivenmicroservices.platform.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.csrf.CsrfWebFilter;
import org.springframework.security.web.server.util.matcher.AndServerWebExchangeMatcher;
import org.springframework.security.web.server.util.matcher.NegatedServerWebExchangeMatcher;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Arrays;
import java.util.Collections;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DEFAULT_SUBJECT = "eventdrivenmicroservices-edge-node";

    @Value("${JWT_SECRET_KEY:#{null}}")
    private String jwtSecretKey;

    @Value("${JWT_ISSUER:#{null}}")
    private String jwtIssuer;

    @Value("${JWT_AUDIENCE:#{null}}")
    private String jwtAudience;

    @Value("${JWT_CLOCK_SKEW_SECONDS:30}")
    private long clockSkewSeconds = 30;

    @Value("${spring.profiles.active:}")
    private String activeProfiles;

    @PostConstruct
    void validateJwtConfiguration() {
        boolean secretRequired = Arrays.stream(activeProfiles.split(","))
                .map(String::trim)
                .anyMatch(profile -> profile.equals("k8s") || profile.equals("prod"));
        if (secretRequired && (jwtSecretKey == null || jwtSecretKey.isBlank())) {
            throw new IllegalStateException("JWT_SECRET_KEY must be configured for k8s and prod profiles");
        }
    }

    @Bean
    public SecurityWebFilterChain filterChain(ServerHttpSecurity http) {
        http
            .csrf(csrf -> csrf.requireCsrfProtectionMatcher(
                new AndServerWebExchangeMatcher(
                    CsrfWebFilter.DEFAULT_CSRF_MATCHER,
                    new NegatedServerWebExchangeMatcher(
                        ServerWebExchangeMatchers.pathMatchers("/api/v1/telemetry", "/api/v1/transactions/**", "/api/v1/loans/**", "/actuator/**")
                    )
                )
            ))
            .addFilterAt(jwtBearerWebFilter(), SecurityWebFiltersOrder.AUTHENTICATION)
            .authorizeExchange(auth -> auth
                .pathMatchers("/", "/index.html", "/app.js", "/styles.css").permitAll()
                .pathMatchers("/actuator/health", "/actuator/health/**", "/actuator/prometheus").permitAll() // Allow healthchecks & metrics scraping without auth
                .pathMatchers("/api/v1/telemetry", "/api/v1/transactions/**", "/api/v1/loans/**").authenticated() // Require authentication for ingestion & loans
                .pathMatchers("/actuator/**").hasRole("ADMIN") // Require ADMIN role for management
                .anyExchange().authenticated()
            )
            .httpBasic(Customizer.withDefaults()); // Basic authentication for edge nodes & operators
            
        return http.build();
    }

    private WebFilter jwtBearerWebFilter() {
        return (ServerWebExchange exchange, WebFilterChain chain) -> {
            String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
            if (authHeader != null && authHeader.startsWith("Bearer ")) {
                String subject = authenticatedSubject(authHeader.substring(7), jwtSecretKey, Instant.now());
                if (subject != null) {
                    // Bearer tokens identify edge nodes only; they never carry operator/ADMIN authorities.
                    UsernamePasswordAuthenticationToken authentication =
                            new UsernamePasswordAuthenticationToken(subject, null, Collections.emptyList());
                    return chain.filter(exchange)
                            .contextWrite(ReactiveSecurityContextHolder.withSecurityContext(Mono.just(new SecurityContextImpl(authentication))));
                }
            }
            return chain.filter(exchange);
        };
    }

    boolean validateJwtToken(String token, String secret) {
        return authenticatedSubject(token, secret, Instant.now()) != null;
    }

    /**
     * Returns the token subject when the token is a valid HS256 JWT, or null otherwise. Requires a verified
     * signature, an unexpired {@code exp}, a reached {@code nbf} when present, and the configured
     * issuer/audience when those are set.
     */
    String authenticatedSubject(String token, String secret, Instant now) {
        if (secret == null || secret.isBlank() || token == null) return null;
        try {
            String[] parts = token.split("\\.", -1);
            if (parts.length != 3) return null;

            JsonNode header = JSON.readTree(Base64.getUrlDecoder().decode(parts[0]));
            if (!"HS256".equals(header.path("alg").asText())) return null;

            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            sha256Hmac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] expected = sha256Hmac.doFinal((parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8));
            if (!MessageDigest.isEqual(expected, Base64.getUrlDecoder().decode(parts[2]))) return null;

            JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(parts[1]));
            long nowSeconds = now.getEpochSecond();
            JsonNode exp = claims.path("exp");
            if (!exp.isNumber() || nowSeconds - clockSkewSeconds >= exp.asLong()) return null;
            JsonNode nbf = claims.path("nbf");
            if (nbf.isNumber() && nowSeconds + clockSkewSeconds < nbf.asLong()) return null;
            if (hasText(jwtIssuer) && !jwtIssuer.equals(claims.path("iss").asText())) return null;
            if (hasText(jwtAudience) && !audienceMatches(claims.path("aud"), jwtAudience)) return null;

            String subject = claims.path("sub").asText("");
            return subject.isBlank() ? DEFAULT_SUBJECT : subject;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean audienceMatches(JsonNode aud, String expected) {
        if (aud.isTextual()) return expected.equals(aud.asText());
        if (aud.isArray()) {
            for (JsonNode value : aud) {
                if (expected.equals(value.asText())) return true;
            }
        }
        return false;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
