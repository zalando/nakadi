package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;

    private final List<Producer<byte[], byte[]>> producers;

    private final BlockingQueue<Consumer<byte[], byte[]>> consumerPool;
    private final Meter consumerCreateMeter;
    private final Meter consumerPoolTakeMeter;
    private final Meter consumerPoolReturnMeter;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager,
                        final MetricRegistry metricsRegistry,
                        final int numActiveProducers,
                        final int consumerPoolSize) {
        this.kafkaLocationManager = kafkaLocationManager;

        LOG.info("Allocating {} Kafka producers", numActiveProducers);
        this.producers = new ArrayList<>(numActiveProducers);
        for (int i = 0; i < numActiveProducers; ++i) {
            this.producers.add(createProducerInstance());
        }

        if (consumerPoolSize > 0) {
            LOG.info("Preparing timelag checker pool of {} Kafka consumers", consumerPoolSize);
            this.consumerPool = new LinkedBlockingQueue(consumerPoolSize);
            for (int i = 0; i < consumerPoolSize; ++i) {
                this.consumerPool.add(createConsumerProxyInstance());
            }
        } else {
            this.consumerPool = null;
        }

        this.consumerCreateMeter = metricsRegistry.meter("nakadi.kafka.consumer.created");
        this.consumerPoolTakeMeter = metricsRegistry.meter("nakadi.kafka.consumer.taken");
        this.consumerPoolReturnMeter = metricsRegistry.meter("nakadi.kafka.consumer.returned");
    }

    public Producer<byte[], byte[]> takeProducer(final String topic) {
        final int index = Math.abs(topic.hashCode() % producers.size());
        return producers.get(index);
    }

    public Consumer<byte[], byte[]> getConsumer(final String clientId /* ignored */) {
        return getConsumer();
    }

    private Consumer<byte[], byte[]> takeConsumer() {
        final Consumer<byte[], byte[]> consumer;

        LOG.trace("Taking timelag consumer from the pool");
        try {
            consumer = consumerPool.poll(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for a consumer from the pool");
        }
        if (consumer == null) {
            throw new RuntimeException("timed out while waiting for a consumer from the pool");
        }

        consumerPoolTakeMeter.mark();

        return consumer;
    }

    private void returnConsumer(final Consumer<byte[], byte[]> consumer) {
        LOG.trace("Returning timelag consumer to the pool");

        consumer.assign(Collections.emptyList());

        consumerPoolReturnMeter.mark();

        try {
            consumerPool.put(consumer);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while putting a consumer back to the pool");
        }
    }

    public Consumer<byte[], byte[]> getConsumer() {
        if (consumerPool != null) {
            return takeConsumer();
        }

        return getConsumer(kafkaLocationManager.getKafkaConsumerProperties());
    }

    private Consumer<byte[], byte[]> getConsumer(final Properties properties) {

        consumerCreateMeter.mark();

        return new KafkaConsumer<byte[], byte[]>(properties);
    }

    protected Producer<byte[], byte[]> createProducerInstance() {
        return new KafkaProducer<byte[], byte[]>(kafkaLocationManager.getKafkaProducerProperties());
    }

    protected Consumer<byte[], byte[]> createConsumerProxyInstance() {
        return new KafkaConsumerProxy(kafkaLocationManager.getKafkaConsumerProperties());
    }

    public class KafkaConsumerProxy extends KafkaConsumer<byte[], byte[]> {

        public KafkaConsumerProxy(final Properties properties) {
            super(properties);
        }

        @Override
        public void close() {
            returnConsumer(this);
        }
    }
}
