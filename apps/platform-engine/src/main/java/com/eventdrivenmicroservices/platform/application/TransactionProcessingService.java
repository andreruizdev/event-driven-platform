package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import reactor.core.publisher.Mono;

public interface TransactionProcessingService {
    Mono<Void> processTransactionAsync(FinancialTransactionPayload payload, String traceparent);
}
