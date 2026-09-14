package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.model.TelemetryPayload;

import reactor.core.publisher.Mono;

public interface TelemetryProcessingService {
    Mono<Void> processPayloadAsync(TelemetryPayload payload);
}
