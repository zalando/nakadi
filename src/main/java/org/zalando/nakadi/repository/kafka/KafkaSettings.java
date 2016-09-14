package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaSettings {

    private final long requestTimeoutMs;
    private final long batchSize;
    private final long lingerMs;

    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.request.timeout.ms}") final long requestTimeoutMs,
                         @Value("${nakadi.kafka.batch.size}") final long batchSize,
                         @Value("${nakadi.kafka.linger.ms}") final long lingerMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public long getLingerMs() {
        return lingerMs;
    }
}
