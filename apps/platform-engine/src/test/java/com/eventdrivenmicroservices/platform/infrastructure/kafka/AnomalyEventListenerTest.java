package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnomalyEventListenerTest {

    @Test
        @SuppressWarnings("unchecked")
    void forwardsTraceparentToAnomalyAlert() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        AnomalyEventListener listener = new AnomalyEventListener(kafkaTemplate);
        RecordHeaders headers = new RecordHeaders();
        headers.add("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1", "ANOMALY detected");
        record.headers().add(headers.lastHeader("traceparent"));

        listener.consumeOutboxEvent(record);

        var producerCaptor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(producerCaptor.capture());
        ProducerRecord<?, ?> alert = producerCaptor.getValue();
        assertEquals("eventdrivenmicroservices-anomaly-alerts", alert.topic());
        assertEquals("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                new String(alert.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8));
    }
}
