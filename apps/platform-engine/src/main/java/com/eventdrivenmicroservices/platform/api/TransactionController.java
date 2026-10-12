package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import com.eventdrivenmicroservices.platform.application.TransactionProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionProcessingService processingService;

    @Autowired
    public TransactionController(TransactionProcessingService processingService) {
        this.processingService = processingService;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> ingestTransaction(
            @Valid @RequestBody FinancialTransactionPayload payload,
            @RequestHeader(value = "traceparent", required = false) String traceparent,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        
        return processingService.processTransactionAsync(payload, traceparent, correlationId)
                .thenReturn(ResponseEntity.accepted().body("Transaction Accepted"))
                .onErrorResume(e -> Mono.just(ResponseEntity.internalServerError().body("Error: " + e.getMessage())));
    }
}
