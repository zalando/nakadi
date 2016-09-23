package org.zalando.nakadi.repository.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class KafkaFactory {

    private final KafkaLocationManager kafkaLocationManager;
    @Nullable
    private Producer<String, String> activeProducer;
    private final Map<Producer<String, String>, AtomicInteger> useCount = new ConcurrentHashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    @Autowired
    public KafkaFactory(final KafkaLocationManager kafkaLocationManager) {
        this.kafkaLocationManager = kafkaLocationManager;
    }


    public Producer<String, String> takeProducer() {
        rwLock.readLock().lock();
        try {
            if (null != activeProducer) {
                useCount.get(activeProducer).incrementAndGet();
                return activeProducer;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        rwLock.writeLock().lock();
        try {
            if (null != activeProducer) {
                useCount.get(activeProducer).incrementAndGet();
                return activeProducer;
            }
            activeProducer = new KafkaProducer<>(kafkaLocationManager.getKafkaProducerProperties());
            useCount.put(activeProducer, new AtomicInteger(1));
            return activeProducer;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void releaseProducer(final Producer<String, String> producer) {
        final int newUseCount = useCount.get(producer).decrementAndGet();
        assert newUseCount >= 0;
        if (newUseCount == 0) {
            final boolean deleteProducer;
            rwLock.readLock().lock();
            try {
                deleteProducer = producer != activeProducer;
            } finally {
                rwLock.readLock().unlock();
            }
            if (deleteProducer) {
                useCount.remove(producer);
                producer.close();
            }
        }
    }

    public void terminateProducer(final Producer<String, String> producer) {
        rwLock.writeLock().lock();
        try {
            if (producer == this.activeProducer) {
                this.activeProducer = null;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(kafkaLocationManager.getKafkaConsumerProperties());
    }

    public NakadiKafkaConsumer createNakadiConsumer(final String topic, final List<KafkaCursor> kafkaCursors,
                                                    final long pollTimeout) {
        return new NakadiKafkaConsumer(getConsumer(), topic, kafkaCursors, pollTimeout);
    }

}
