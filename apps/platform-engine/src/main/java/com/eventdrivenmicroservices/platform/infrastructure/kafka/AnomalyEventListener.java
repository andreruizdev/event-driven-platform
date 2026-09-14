package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AnomalyEventListener {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public AnomalyEventListener(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "eventdrivenmicroservices-outbox-events", groupId = "eventdrivenmicroservices-anomaly-cep-group")
    public void consumeOutboxEvent(ConsumerRecord<String, String> record) {
        String eventPayload = record.value();
        Header traceparentHeader = record.headers().lastHeader("traceparent");
        String traceparent = traceparentHeader == null ? null : new String(traceparentHeader.value(), java.nio.charset.StandardCharsets.UTF_8);
        log.debug("[KAFKA CEP] Consumed outbox event payload: {}", eventPayload);
        log.debug("[KAFKA CEP] Consumed traceparent: {}", traceparent);

        if (eventPayload != null && (eventPayload.contains("AnomalyDetected") || eventPayload.contains("ANOMALY"))) {
            log.warn("[KAFKA CEP ALERT] Anomaly detected in Kafka event stream! Routing alert to topic 'eventdrivenmicroservices-anomaly-alerts'");
            try {
                ProducerRecord<String, String> alert = new ProducerRecord<>(
                        "eventdrivenmicroservices-anomaly-alerts", record.key(), eventPayload);
                if (traceparent != null && !traceparent.isBlank()) {
                    alert.headers().add("traceparent", traceparent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                kafkaTemplate.send(alert);
            } catch (Exception e) {
                log.error("[KAFKA CEP] Failed to route anomaly alert to Kafka: {}", e.getMessage(), e);
            }
        }
    }
}
