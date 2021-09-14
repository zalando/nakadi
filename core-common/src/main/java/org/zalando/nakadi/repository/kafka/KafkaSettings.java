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
    private final long bufferMemory;
    private final int lingerMs;
    private final boolean enableAutoCommit;
    private final int maxRequestSize;
    private final int deliveryTimeoutMs;
    private final int maxBlockMs;
    private final String clientRack;

    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.request.timeout.ms}") final int requestTimeoutMs,
                         @Value("${nakadi.kafka.batch.size}") final int batchSize,
                         @Value("${nakadi.kafka.buffer.memory}") final long bufferMemory,
                         @Value("${nakadi.kafka.linger.ms}") final int lingerMs,
                         @Value("${nakadi.kafka.enable.auto.commit}") final boolean enableAutoCommit,
                         @Value("${nakadi.kafka.max.request.size}") final int maxRequestSize,
                         @Value("${nakadi.kafka.delivery.timeout.ms}") final int deliveryTimeoutMs,
                         @Value("${nakadi.kafka.max.block.ms}") final int maxBlockMs,
                         @Value("${nakadi.kafka.client.rack:}") final String clientRack) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.batchSize = batchSize;
        this.bufferMemory = bufferMemory;
        this.lingerMs = lingerMs;
        this.enableAutoCommit = enableAutoCommit;
        this.maxRequestSize = maxRequestSize;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.maxBlockMs = maxBlockMs;
        this.clientRack = clientRack;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public int getMaxBlockMs() {
        return maxBlockMs;
    }

    public String getClientRack() {
        return clientRack;
    }
}
