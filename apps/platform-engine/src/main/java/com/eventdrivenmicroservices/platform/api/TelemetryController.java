package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.model.TelemetryPayload;
import com.eventdrivenmicroservices.platform.application.TelemetryProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1")
public class TelemetryController {

    private final TelemetryProcessingService processingService;

    @Autowired
    public TelemetryController(TelemetryProcessingService processingService) {
        this.processingService = processingService;
    }

    @PostMapping("/telemetry")
    public Mono<ResponseEntity<Void>> receiveTelemetry(@RequestBody TelemetryPayload payload) {
        return processingService.processPayloadAsync(payload)
                .thenReturn(ResponseEntity.status(HttpStatus.ACCEPTED).<Void>build())
                .onErrorResume(e -> Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).<Void>build()));
    }
}
