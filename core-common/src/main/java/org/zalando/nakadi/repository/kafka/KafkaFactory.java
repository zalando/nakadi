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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;
    private final Map<Producer<byte[], byte[]>, AtomicInteger> useCount;
    private final ReadWriteLock rwLock;
    private final List<Producer<byte[], byte[]>> activeProducers;
    private final AtomicLong activeProducerCounter;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager, final int numActiveProducers) {
        this.kafkaLocationManager = kafkaLocationManager;

        this.useCount = new ConcurrentHashMap<>();
        this.rwLock = new ReentrantReadWriteLock();

        this.activeProducers = new ArrayList<>(numActiveProducers);
        for (int i = 0; i < numActiveProducers; ++i) {
            this.activeProducers.add(null);
        }

        this.activeProducerCounter = new AtomicLong(0);
    }

    @Nullable
    private Producer<byte[], byte[]> takeUnderLock(final int index, final boolean canCreate) {
        final Lock lock = canCreate ? rwLock.writeLock() : rwLock.readLock();
        lock.lock();
        try {
            Producer<byte[], byte[]> producer = activeProducers.get(index);
            if (null != producer) {
                useCount.get(producer).incrementAndGet();
                return producer;
            } else if (canCreate) {
                producer = createProducerInstance();
                useCount.put(producer, new AtomicInteger(1));
                activeProducers.set(index, producer);

                LOG.info("New producer instance created: " + producer);
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
    public Producer<byte[], byte[]> takeProducer() {
        final int index = (int)(activeProducerCounter.incrementAndGet() % activeProducers.size());

        Producer<byte[], byte[]> result = takeUnderLock(index, false);
        if (null == result) {
            result = takeUnderLock(index, true);
        }
        return result;
    }

    /**
     * Release kafka producer that was obtained by {@link #takeProducer()} method. If producer was not obtained by
     * {@link #takeProducer()} call - method will throw {@link NullPointerException}
     *
     * @param producer Producer to release.
     */
    public void releaseProducer(final Producer<byte[], byte[]> producer) {
        final AtomicInteger counter = useCount.get(producer);
        if (counter != null && 0 == counter.decrementAndGet()) {
            final boolean deleteProducer;
            rwLock.readLock().lock();
            try {
                deleteProducer = !activeProducers.contains(producer);
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
    public void terminateProducer(final Producer<byte[], byte[]> producer) {
        LOG.info("Received signal to terminate producer " + producer);
        rwLock.writeLock().lock();
        try {
            final int index = activeProducers.indexOf(producer);
            if (index >= 0) {
                activeProducers.set(index, null);
            } else {
                LOG.info("Signal for producer termination already received: " + producer);
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

    protected Producer<byte[], byte[]> createProducerInstance() {
        return new KafkaProducerCrutch(kafkaLocationManager.getKafkaProducerProperties(),
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
