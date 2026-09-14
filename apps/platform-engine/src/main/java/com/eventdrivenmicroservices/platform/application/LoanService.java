package com.eventdrivenmicroservices.platform.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.model.LoanApplication;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LoanApplicationRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class LoanService {
    private final LoanApplicationRepository loanRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final DynamicLoanValidator dynamicLoanValidator;
    private final LedgerService ledgerService;

    private static final TextMapSetter<Map<String, String>> setter =
            (carrier, key, value) -> carrier.put(key, value);

    @Transactional
    public Mono<LoanApplication> submitApplication(LoanApplicationRequest request) {
        dynamicLoanValidator.validate(request);

        LoanApplication application = new LoanApplication();
        application.setApplicantId(request.getApplicantId());
        application.setAmount(request.getAmount());
        application.setTermMonths(request.getTermMonths());
        application.setCreatedAt(Instant.now());
        application.setStatus("PENDING_REVIEW");

        return loanRepository.save(application)
            .flatMap(savedApp -> {
                String payload;
                try {
                    payload = objectMapper.writeValueAsString(savedApp);
                } catch (JsonProcessingException e) {
                    return Mono.error(new RuntimeException("Failed to serialize loan application", e));
                }

                Map<String, String> traceContext = new HashMap<>();
                GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), traceContext, setter);
                String traceparent = traceContext.get("traceparent");

                OutboxEvent event = new OutboxEvent();
                event.setAggregateType("LoanApplication");
                event.setAggregateId(savedApp.getId().toString());
                event.setEventType("ApplicationSubmitted");
                event.setPayload(payload);
                event.setTraceparent(traceparent);
                event.setCreatedAt(Instant.now());

                return outboxRepository.save(event)
                        .flatMap(savedEvent -> ledgerService.appendToLedger("ApplicationSubmitted", payload))
                        .thenReturn(savedApp);
            });
    }
}
