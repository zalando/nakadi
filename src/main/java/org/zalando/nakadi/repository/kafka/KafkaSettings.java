package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaSettings {

    private final int requestTimeoutMs;
    private final int batchSize;
    private final long lingerMs;

    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.request.timeout.ms}") final int requestTimeoutMs,
                         @Value("${nakadi.kafka.batch.size}") final int batchSize,
                         @Value("${nakadi.kafka.linger.ms}") final long lingerMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getLingerMs() {
        return lingerMs;
    }
}
