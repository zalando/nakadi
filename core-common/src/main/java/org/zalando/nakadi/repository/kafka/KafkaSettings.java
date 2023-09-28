package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KafkaSettings {

    private final int retries;
    private final boolean idempotence;

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
    private final int maxInFlightRequests;
    private final boolean enableAutoCommit;
    private final int maxRequestSize;
    private final int deliveryTimeoutMs;
    private final int maxBlockMs;
    private final String clientRack;
    private final String compressionType;
    private final int socketSendBufferBytes;
    private final Optional<Integer> preferredListenerPort;
    private final Optional<String> securityProtocol;
    private final Optional<String> saslMechanism;
    private final Optional<String> kafkaUsername;
    private final Optional<String> kafkaPassword;
    @Autowired
    public KafkaSettings(@Value("${nakadi.kafka.retries}") final int retries,
                         @Value("${nakadi.kafka.idempotence}") final boolean idempotence,
                         @Value("${nakadi.kafka.request.timeout.ms}") final int requestTimeoutMs,
                         @Value("${nakadi.kafka.batch.size}") final int batchSize,
                         @Value("${nakadi.kafka.buffer.memory}") final long bufferMemory,
                         @Value("${nakadi.kafka.linger.ms}") final int lingerMs,
                         @Value("${nakadi.kafka.max.in.flight.requests.per.connection}") final int maxInFlightRequests,
                         @Value("${nakadi.kafka.enable.auto.commit}") final boolean enableAutoCommit,
                         @Value("${nakadi.kafka.max.request.size}") final int maxRequestSize,
                         @Value("${nakadi.kafka.delivery.timeout.ms}") final int deliveryTimeoutMs,
                         @Value("${nakadi.kafka.max.block.ms}") final int maxBlockMs,
                         @Value("${nakadi.kafka.client.rack:}") final String clientRack,
                         @Value("${nakadi.kafka.compression.type:lz4}") final String compressionType,
                         @Value("${nakadi.kafka.socket.send.buffer.bytes:}") final int socketSendBufferBytes,
                         @Value("${nakadi.kafka.preferred.listener.port:#{null}}")
                             final Optional<Integer> preferredListenerPort,
                         @Value("${nakadi.kafka.security.protocol:#{null}}") final Optional<String> securityProtocol,
                         @Value("${nakadi.kafka.sasl.mechanism:#{null}}") final Optional<String> saslMechanism,
                         @Value("${nakadi.kafka.username:#{null}}") final Optional<String> kafkaUsername,
                         @Value("${nakadi.kafka.password:#{null}}") final Optional<String> kafkaPassword) {
        this.retries = retries;
        this.idempotence = idempotence;
        this.requestTimeoutMs = requestTimeoutMs;
        this.batchSize = batchSize;
        this.bufferMemory = bufferMemory;
        this.lingerMs = lingerMs;
        this.maxInFlightRequests = maxInFlightRequests;
        this.enableAutoCommit = enableAutoCommit;
        this.maxRequestSize = maxRequestSize;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.maxBlockMs = maxBlockMs;
        this.clientRack = clientRack;
        this.compressionType = compressionType;
        this.socketSendBufferBytes = socketSendBufferBytes;
        this.preferredListenerPort = preferredListenerPort;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.kafkaUsername = kafkaUsername;
        this.kafkaPassword = kafkaPassword;
    }

    public int getRetries() {
        return retries;
    }

    public boolean getIdempotence() {
        return idempotence;
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

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
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

    public String getCompressionType() {
        return compressionType;
    }

    public int getSocketSendBufferBytes() {
        return socketSendBufferBytes;
    }

    public Optional<Integer> getPreferredListenerPort() {
        return preferredListenerPort;
    }

    public Optional<String> getSecurityProtocol() {
        return securityProtocol;
    }

    public Optional<String> getSaslMechanism() {
        return saslMechanism;
    }

    public Optional<String> getKafkaUsername() {
        return kafkaUsername;
    }

    public Optional<String> getKafkaPassword() {
        return kafkaPassword;
    }
}
