package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;

    private final Map<Producer<byte[], byte[]>, AtomicInteger> useCount;
    private final ReadWriteLock rwLock;
    private final List<Producer<byte[], byte[]>> activeProducers;

    private final BlockingQueue<Consumer<byte[], byte[]>> consumerPool;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager,
                        final int numActiveProducers,
                        final int consumerPoolSize) {
        this.kafkaLocationManager = kafkaLocationManager;

        this.useCount = new ConcurrentHashMap<>();
        this.rwLock = new ReentrantReadWriteLock();

        LOG.info("Allocating up to {} active Kafka producers", numActiveProducers);
        this.activeProducers = new ArrayList<>(numActiveProducers);
        for (int i = 0; i < numActiveProducers; ++i) {
            this.activeProducers.add(null);
        }

        LOG.info("Preparing timelag checker pool of {} Kafka consumers", consumerPoolSize);
        this.consumerPool = new LinkedBlockingQueue(consumerPoolSize);
        for (int i = 0; i < consumerPoolSize; ++i) {
            this.consumerPool.add(createConsumerProxyInstance());
        }
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
    public Producer<byte[], byte[]> takeProducer(final String topic) {
        final int index = Math.abs(topic.hashCode() % activeProducers.size());

        Producer<byte[], byte[]> result = takeUnderLock(index, false);
        if (null == result) {
            result = takeUnderLock(index, true);
        }
        return result;
    }

    /**
     * Release kafka producer that was obtained by {@link #takeProducer(String)} method. If producer was not obtained by
     * {@link #takeProducer(String)} call - method will throw {@link NullPointerException}
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
     * producer instance right now can continue using it, but new calls to {@link #takeProducer(String)}
     * will use some other producers.
     * It is allowed to call this method only between {@link #takeProducer(String)} and
     * {@link #releaseProducer(Producer)} method calls.
     * (You can not terminate something that you do not own)
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

    public Consumer<byte[], byte[]> getConsumer(final String clientId) {
        // HACK: See SubscriptionTimeLagService
        if (clientId == null || !clientId.startsWith("time-lag-checker-")) {
            return getConsumer();
        }

        LOG.trace("Taking timelag consumer from the pool");
        final Consumer<byte[], byte[]> consumer;
        try {
            consumer = consumerPool.poll(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for a consumer from the pool");
        }
        if (consumer == null) {
            throw new RuntimeException("timed out while waiting for a consumer from the pool");
        }

        return consumer;
    }

    public void returnConsumer(final Consumer<byte[], byte[]> consumer) {
        LOG.trace("Returning timelag consumer to the pool");

        consumer.assign(Collections.emptyList());

        try {
            consumerPool.put(consumer);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while putting a consumer back to the pool");
        }
    }

    public Consumer<byte[], byte[]> getConsumer() {
        return getConsumer(kafkaLocationManager.getKafkaConsumerProperties());
    }

    private Consumer<byte[], byte[]> getConsumer(final Properties properties) {
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
