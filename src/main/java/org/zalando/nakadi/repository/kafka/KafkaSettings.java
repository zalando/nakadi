package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaSettings {

    // kafka client requires this property to be int
    // https://github.com/apache/kafka/blob/d9206500bf2f99ce93f6ad64c7a89483100b3b5f/clients/src/main/java/org/apache
    // /kafka/clients/producer/ProducerConfig.java#L261
    private final int requestTimeoutMs;
    // kafka client requires this property to be int
    // https://github.com/apache/kafka/blob/d9206500bf2f99ce93f6ad64c7a89483100b3b5f/clients/src/main/java/org/apache
    // /kafka/clients/producer/ProducerConfig.java#L232
    private final int batchSize;
    private final long lingerMs;
    private final boolean enableAutoCommit;

    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.request.timeout.ms}") final int requestTimeoutMs,
                         @Value("${nakadi.kafka.batch.size}") final int batchSize,
                         @Value("${nakadi.kafka.linger.ms}") final long lingerMs,
                         @Value("${nakadi.kafka.enable.auto.commit}") final boolean enableAutoCommit) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.enableAutoCommit = enableAutoCommit;
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

    public boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }
}
