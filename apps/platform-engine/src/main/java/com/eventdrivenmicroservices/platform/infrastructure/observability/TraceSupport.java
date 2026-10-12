package com.eventdrivenmicroservices.platform.infrastructure.observability;

import io.micrometer.observation.Observation;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.List;

/**
 * Carries one W3C trace across the HTTP -> outbox -> Kafka -> consumer hops. Reactive code has no thread-local
 * span, so the parent is read from the Reactor context that WebFlux populates with the server observation.
 */
@Component
public class TraceSupport {

    private static final TextMapGetter<String> TRACEPARENT_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(String carrier) {
            return List.of("traceparent");
        }

        @Override
        public String get(String carrier, String key) {
            return "traceparent".equals(key) ? carrier : null;
        }
    };

    private final Tracer tracer;
    private final io.opentelemetry.api.trace.Tracer otelTracer;

    public TraceSupport(Tracer tracer, io.opentelemetry.api.trace.Tracer otelTracer) {
        this.tracer = tracer;
        this.otelTracer = otelTracer;
    }

    public static TraceSupport noop() {
        return new TraceSupport(Tracer.NOOP, OpenTelemetry.noop().getTracer("noop"));
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** The traceparent of the span serving the current request, or empty when tracing is inactive. */
    public Mono<String> currentTraceparent() {
        return Mono.deferContextual(ctx -> Mono.justOrEmpty(parentOf(ctx)).map(TraceSupport::format));
    }

    /** Runs the publisher inside a child span of the current request span; passes through when untraced. */
    public <T> Mono<T> traced(String name, Mono<T> source) {
        return Mono.deferContextual(ctx -> {
            TraceContext parent = parentOf(ctx);
            if (parent == null) {
                return source;
            }
            Span span = tracer.spanBuilder().setParent(parent).name(name).start();
            return source.doOnError(span::error).doFinally(signal -> span.end());
        });
    }

    /** Runs blocking work inside a span whose parent is the supplied traceparent (new root if absent). */
    public void runInSpan(String name, String traceparent, String tagKey, String tagValue, ThrowingRunnable work)
            throws Exception {
        Context parent = Context.root();
        if (traceparent != null && !traceparent.isBlank()) {
            parent = W3CTraceContextPropagator.getInstance()
                    .extract(Context.root(), traceparent.trim(), TRACEPARENT_GETTER);
        }
        io.opentelemetry.api.trace.Span span = otelTracer.spanBuilder(name).setParent(parent).startSpan();
        if (tagKey != null && tagValue != null) {
            span.setAttribute(tagKey, tagValue);
        }
        try (Scope ignored = span.makeCurrent()) {
            work.run();
        } catch (Exception | Error e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    private TraceContext parentOf(ContextView ctx) {
        Observation observation = ctx.getOrDefault(ObservationThreadLocalAccessor.KEY, null);
        if (observation != null) {
            TracingObservationHandler.TracingContext tracing =
                    observation.getContextView().get(TracingObservationHandler.TracingContext.class);
            if (tracing != null && tracing.getSpan() != null) {
                return tracing.getSpan().context();
            }
        }
        Span current = tracer.currentSpan();
        return current == null ? null : current.context();
    }

    static String format(TraceContext context) {
        String traceId = "0".repeat(Math.max(0, 32 - context.traceId().length())) + context.traceId();
        return "00-" + traceId + "-" + context.spanId() + "-" + (Boolean.TRUE.equals(context.sampled()) ? "01" : "00");
    }
}
