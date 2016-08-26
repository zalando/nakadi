package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaSettings {

    private final long kafkaPollTimeoutMs;
    private final long kafkaSendTimeoutMs;

    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.poll.timeoutMs}") final long kafkaPollTimeoutMs,
                         @Value("${nakadi.kafka.send.timeoutMs}") final long kafkaSendTimeoutMs) {
        this.kafkaPollTimeoutMs = kafkaPollTimeoutMs;
        this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
    }

    public long getKafkaPollTimeoutMs() {
        return kafkaPollTimeoutMs;
    }

    public long getKafkaSendTimeoutMs() {
        return kafkaSendTimeoutMs;
    }
}
