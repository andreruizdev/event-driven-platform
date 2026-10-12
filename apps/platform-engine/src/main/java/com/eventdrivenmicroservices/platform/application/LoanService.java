package com.eventdrivenmicroservices.platform.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.model.LoanApplication;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LoanApplicationRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.observability.TraceSupport;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class LoanService {
    private final LoanApplicationRepository loanRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final DynamicLoanValidator dynamicLoanValidator;
    private final LedgerService ledgerService;
    private final TraceSupport traceSupport;

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

                return traceSupport.currentTraceparent().defaultIfEmpty("").flatMap(traceparent -> {
                    OutboxEvent event = new OutboxEvent();
                    event.setAggregateType("LoanApplication");
                    event.setAggregateId(savedApp.getId().toString());
                    event.setEventType("ApplicationSubmitted");
                    event.setPayload(payload);
                    event.setTraceparent(traceparent.isEmpty() ? null : traceparent);
                    event.setCreatedAt(Instant.now());

                    return outboxRepository.save(event)
                            .flatMap(savedEvent -> traceSupport.traced("ledger.append",
                                    ledgerService.appendToLedger("ApplicationSubmitted", payload)))
                            .thenReturn(savedApp);
                });
            });
    }
}
