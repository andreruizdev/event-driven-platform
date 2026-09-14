package com.eventdrivenmicroservices.platform.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
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
import java.util.Base64;
import java.util.Collections;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Value("${JWT_SECRET_KEY:#{null}}")
    private String jwtSecretKey;

    @Bean
    public SecurityWebFilterChain filterChain(ServerHttpSecurity http) {
        http
            .csrf(csrf -> csrf.disable()) // Disable CSRF for stateless REST APIs
            .addFilterAt(jwtBearerWebFilter(), SecurityWebFiltersOrder.AUTHENTICATION)
            .authorizeExchange(auth -> auth
                .pathMatchers("/", "/index.html", "/app.js", "/styles.css").permitAll()
                .pathMatchers("/actuator/health", "/actuator/health/**").permitAll() // Allow healthchecks without auth
                .pathMatchers("/api/v1/telemetry", "/api/v1/transactions/**", "/api/v1/loans/**").authenticated() // Require authentication for ingestion & loans
                .pathMatchers("/actuator/**").hasRole("ADMIN") // Require ADMIN role for metrics management
                .anyExchange().authenticated()
            )
            .httpBasic(Customizer.withDefaults()); // Basic authentication for edge nodes & operators
            
        return http.build();
    }

    private WebFilter jwtBearerWebFilter() {
        return (ServerWebExchange exchange, WebFilterChain chain) -> {
            String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
            if (authHeader != null && authHeader.startsWith("Bearer ")) {
                String token = authHeader.substring(7);
                if (validateJwtToken(token, jwtSecretKey)) {
                    UsernamePasswordAuthenticationToken authentication =
                            new UsernamePasswordAuthenticationToken("eventdrivenmicroservices-edge-node", null, Collections.emptyList());
                    return chain.filter(exchange)
                            .contextWrite(ReactiveSecurityContextHolder.withSecurityContext(Mono.just(new SecurityContextImpl(authentication))));
                }
            }
            return chain.filter(exchange);
        };
    }

    private boolean validateJwtToken(String token, String secret) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) return false;
            String headerAndPayload = parts[0] + "." + parts[1];
            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            sha256Hmac.init(secretKey);
            byte[] signatureBytes = sha256Hmac.doFinal(headerAndPayload.getBytes(StandardCharsets.UTF_8));
            String expectedSignature = Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
            return expectedSignature.equals(parts[2]);
        } catch (Exception e) {
            return false;
        }
    }
}
