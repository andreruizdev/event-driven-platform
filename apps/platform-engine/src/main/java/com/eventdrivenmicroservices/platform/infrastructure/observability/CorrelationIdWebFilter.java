package com.eventdrivenmicroservices.platform.infrastructure.observability;

import io.opentelemetry.api.trace.Span;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdWebFilter implements WebFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String TRACE_ID_HEADER = "X-Trace-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        String traceId = Span.current().getSpanContext().getTraceId();
        exchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, correlationId);
        if (traceId != null && !traceId.isBlank() && !traceId.equals("00000000000000000000000000000000")) {
            exchange.getResponse().getHeaders().set(TRACE_ID_HEADER, traceId);
        }

        return chain.filter(exchange);
    }
}
