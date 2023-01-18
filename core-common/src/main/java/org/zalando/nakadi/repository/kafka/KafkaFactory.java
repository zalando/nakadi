package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaFactory {

    public static final String DEFAULT_PRODUCER_CLIENT_ID = "default";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;

    private final ReadWriteLock rwLock;
    private final Map<Producer<byte[], byte[]>, AtomicInteger> useCountByProducer;
    private final Map<String, Producer<byte[], byte[]>> activeProducerByClientId;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager) {
        this.kafkaLocationManager = kafkaLocationManager;

        this.rwLock = new ReentrantReadWriteLock();
        this.useCountByProducer = new ConcurrentHashMap<>();
        this.activeProducerByClientId = new HashMap<>();
    }

    @Nullable
    private Producer<byte[], byte[]> takeUnderLock(final String clientId, final boolean canCreate) {
        final Lock lock = canCreate ? rwLock.writeLock() : rwLock.readLock();
        lock.lock();
        try {
            Producer<byte[], byte[]> producer = activeProducerByClientId.get(clientId);
            if (null != producer) {
                useCountByProducer.get(producer).incrementAndGet();
                return producer;
            } else if (canCreate) {
                producer = createProducerInstance(clientId);
                useCountByProducer.put(producer, new AtomicInteger(1));
                activeProducerByClientId.put(clientId, producer);

                LOG.info("New producer instance created with client id '{}': {}", clientId, producer);
                return producer;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes producer from producer cache. Every producer, that was received by this method must be released with
     * {@link #releaseProducer(Producer)} method.
     *
     * @return Initialized kafka producer instance.
     */
    public Producer<byte[], byte[]> takeProducer(final String clientId) {
        Producer<byte[], byte[]> result = takeUnderLock(clientId, false);
        if (null == result) {
            result = takeUnderLock(clientId, true);
        }
        return result;
    }

    public Producer<byte[], byte[]> takeDefaultProducer() {
        return takeProducer(DEFAULT_PRODUCER_CLIENT_ID);
    }

    /**
     * Release kafka producer that was obtained by {@link #takeProducer()} method. If producer was not obtained by
     * {@link #takeProducer()} call - method will throw {@link NullPointerException}
     *
     * @param producer Producer to release.
     */
    public void releaseProducer(final Producer<byte[], byte[]> producer) {
        final AtomicInteger counter = useCountByProducer.get(producer);
        if (counter != null && 0 == counter.decrementAndGet()) {
            final boolean deleteProducer;
            rwLock.readLock().lock();
            try {
                deleteProducer = !activeProducerByClientId.containsValue(producer);
            } finally {
                rwLock.readLock().unlock();
            }
            if (deleteProducer) {
                rwLock.writeLock().lock();
                try {
                    if (counter.get() == 0 && null != useCountByProducer.remove(producer)) {
                        LOG.info("Stopping producer instance - It was reported that instance should be refreshed " +
                                "and it is not used anymore: {}", producer);
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
    public void terminateProducer(final Producer<byte[], byte[]> producer) {
        LOG.info("Received signal to terminate producer: {}", producer);
        rwLock.writeLock().lock();
        try {
            final Optional<String> clientId = activeProducerByClientId.entrySet().stream()
                    .filter(kv -> kv.getValue() == producer)
                    .map(Map.Entry::getKey)
                    .findFirst();
            if (clientId.isPresent()) {
                activeProducerByClientId.remove(clientId.get());
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Consumer<byte[], byte[]> getConsumer(final Properties properties) {
        return new KafkaCrutchConsumer(properties, new KafkaCrutch(kafkaLocationManager));
    }

    public Consumer<byte[], byte[]> getConsumer() {
        return getConsumer(kafkaLocationManager.getKafkaConsumerProperties());
    }

    public Consumer<byte[], byte[]> getConsumer(@Nullable final String clientId) {
        final Properties properties = kafkaLocationManager.getKafkaConsumerProperties();
        return this.getConsumer(properties);
    }

    public class KafkaCrutchConsumer extends KafkaConsumer<byte[], byte[]> {

        private final KafkaCrutch kafkaCrutch;

        public KafkaCrutchConsumer(final Properties properties, final KafkaCrutch kafkaCrutch) {
            super(properties);
            this.kafkaCrutch = kafkaCrutch;
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(final long timeoutMs) {
            if (kafkaCrutch.brokerIpAddressChanged) {
                throw new KafkaCrutchException("Kafka broker ip address changed, exiting");
            }
            return super.poll(timeoutMs);
        }

        @Override
        public void close() {
            kafkaCrutch.close();
            super.close();
        }
    }

    public class KafkaCrutchException extends RuntimeException {
        public KafkaCrutchException(final String message) {
            super(message);
        }
    }

    public class KafkaProducerCrutch extends KafkaProducer<byte[], byte[]> {

        private final KafkaCrutch kafkaCrutch;

        public KafkaProducerCrutch(final Properties properties, final KafkaCrutch kafkaCrutch) {
            super(properties);
            this.kafkaCrutch = kafkaCrutch;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
            if (kafkaCrutch.brokerIpAddressChanged) {
                throw new KafkaCrutchException("Kafka broker ip address changed, exiting");
            }
            return super.send(record, callback);
        }

        @Override
        public void close() {
            kafkaCrutch.close();
            super.close();
        }
    }

    protected Producer<byte[], byte[]> createProducerInstance(final String clientId) {
        return new KafkaProducerCrutch(kafkaLocationManager.getKafkaProducerProperties(clientId),
                new KafkaCrutch(kafkaLocationManager));
    }

    private class KafkaCrutch implements Closeable {

        private final KafkaLocationManager kafkaLocationManager;
        private final Runnable brokerIpAddressChangeListener;
        private volatile boolean brokerIpAddressChanged;

        KafkaCrutch(final KafkaLocationManager kafkaLocationManager) {
            this.kafkaLocationManager = kafkaLocationManager;
            this.brokerIpAddressChangeListener = () -> brokerIpAddressChanged = true;
            this.kafkaLocationManager.addIpAddressChangeListener(brokerIpAddressChangeListener);
        }

        @Override
        public void close() {
            kafkaLocationManager.removeIpAddressChangeListener(brokerIpAddressChangeListener);
        }
    }
}
