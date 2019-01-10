package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;
    private final Counter useCountMetric;
    private final Counter producerTerminations;
    private final Map<Producer<String, String>, AtomicInteger> useCount = new ConcurrentHashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @Nullable
    private Producer<String, String> activeProducer;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager, final MetricRegistry metricRegistry) {
        this.kafkaLocationManager = kafkaLocationManager;
        this.useCountMetric = metricRegistry.counter("kafka.producer.use_count");
        this.producerTerminations = metricRegistry.counter("kafka.producer.termination_count");
    }

    @Nullable
    private Producer<String, String> takeUnderLock(final boolean canCreate) {
        final Lock lock = canCreate ? rwLock.writeLock() : rwLock.readLock();
        lock.lock();
        try {
            if (null != activeProducer) {
                useCount.get(activeProducer).incrementAndGet();
                return activeProducer;
            } else if (canCreate) {
                activeProducer = createProducerInstance();
                useCount.put(activeProducer, new AtomicInteger(1));
                LOG.info("New producer instance created: " + activeProducer);
                return activeProducer;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    protected Producer<String, String> createProducerInstance() {
        return new KafkaProducer<>(kafkaLocationManager.getKafkaProducerProperties());
    }

    /**
     * Takes producer from producer cache. Every producer, that was received by this method must be released with
     * {@link #releaseProducer(Producer)} method.
     *
     * @return Initialized kafka producer instance.
     */
    public Producer<String, String> takeProducer() {
        Producer<String, String> result = takeUnderLock(false);
        if (null == result) {
            result = takeUnderLock(true);
        }
        useCountMetric.inc();
        return result;
    }

    /**
     * Release kafka producer that was obtained by {@link #takeProducer()} method. If producer was not obtained by
     * {@link #takeProducer()} call - method will throw {@link NullPointerException}
     *
     * @param producer Producer to release.
     */
    public void releaseProducer(final Producer<String, String> producer) {
        useCountMetric.dec();
        final AtomicInteger counter = useCount.get(producer);
        if (counter != null && 0 == counter.decrementAndGet()) {
            final boolean deleteProducer;
            rwLock.readLock().lock();
            try {
                deleteProducer = producer != activeProducer;
            } finally {
                rwLock.readLock().unlock();
            }
            if (deleteProducer) {
                rwLock.writeLock().lock();
                try {
                    if (counter.get() == 0 && null != useCount.remove(producer)) {
                        LOG.info("Stopping producer instance - It was reported that instance should be refreshed " +
                                "and it is not used anymore: " + producer);
                        producer.close();
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        }
    }

    /**
     * Notifies producer cache, that this producer should be marked as obsolete. All methods, that are using this
     * producer instance right now can continue using it, but new calls to {@link #takeProducer()} will use some other
     * producers.
     * It is allowed to call this method only between {@link #takeProducer()} and {@link #releaseProducer(Producer)}
     * method calls. (You can not terminate something that you do not own)
     *
     * @param producer Producer instance to terminate.
     */
    public void terminateProducer(final Producer<String, String> producer) {
        LOG.info("Received signal to terminate producer " + producer);
        rwLock.writeLock().lock();
        try {
            if (producer == this.activeProducer) {
                producerTerminations.inc();
                this.activeProducer = null;
            } else {
                LOG.info("Signal for producer termination already received: " + producer);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Consumer<byte[], byte[]> getConsumer(final Properties properties) {
        return new KafkaCrutchConsumer(properties, kafkaLocationManager);
    }

    public Consumer<byte[], byte[]> getConsumer() {
        return getConsumer(kafkaLocationManager.getKafkaConsumerProperties());
    }

    public Consumer<byte[], byte[]> getConsumer(@Nullable final String clientId) {
        final Properties properties = kafkaLocationManager.getKafkaConsumerProperties();
        // TODO: the line bellow has been commented after a bug in Kafka's 0.9.x throttling feature has been detected.
        // once Kafka is upgraded, we are going to enable it back. More on how it works can be found at
        // https://docs.google.com/document/d/1JDgsBemNqS0SrNpWUL90205u0MFmSMnOqrC-ENAb6TM/edit
        // properties.put("client.id", clientId);
        return this.getConsumer(properties);
    }

    public class KafkaCrutchConsumer extends KafkaConsumer<byte[], byte[]> {

        private final KafkaLocationManager kafkaLocationManager;
        private final Runnable brokerIpAddressChangeListener;
        private volatile boolean brokerIpAddressChanged;

        public KafkaCrutchConsumer(final Properties properties, final KafkaLocationManager kafkaLocationManager) {
            super(properties);
            this.kafkaLocationManager = kafkaLocationManager;
            this.brokerIpAddressChangeListener = () -> brokerIpAddressChanged = true;
            this.kafkaLocationManager.addIpAddressChangeListener(brokerIpAddressChangeListener);
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(final long timeoutMs) {
            if (brokerIpAddressChanged) {
                throw new KafkaCrutchException("Kafka broker ip address changed, exiting");
            }
            return super.poll(timeoutMs);
        }

        @Override
        public void close() {
            if (brokerIpAddressChanged) {
                LOG.info("Closing crutch consumer and removing ip address change listener");
            }
            this.kafkaLocationManager.removeIpAddressChangeListener(brokerIpAddressChangeListener);
            super.close();
        }
    }

    public class KafkaCrutchException extends RuntimeException {
        public KafkaCrutchException(final String message) {
            super(message);
        }
    }

}
