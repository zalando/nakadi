package org.zalando.nakadi.service.timeline;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.repository.kafka.KafkaPartitionsCalculator;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageWorkerFactory {

    private final ConcurrentHashMap<String, StorageWorker> storages = new ConcurrentHashMap<>();
    private final KafkaPartitionsCalculator kafkaPartitionsCalculator;
    private final NakadiSettings nakadiSettings;
    private final MetricRegistry metricRegistry;
    private final ZookeeperSettings zookeeperSettings;
    private final KafkaSettings kafkaSettings;
    private final UUIDGenerator uuidGenerator;

    @Autowired
    public StorageWorkerFactory(final KafkaPartitionsCalculator kafkaPartitionsCalculator,
                                final NakadiSettings nakadiSettings,
                                final MetricRegistry metricRegistry,
                                final ZookeeperSettings zookeeperSettings,
                                final KafkaSettings kafkaSettings,
                                final UUIDGenerator uuidGenerator) {
        this.kafkaPartitionsCalculator = kafkaPartitionsCalculator;
        this.nakadiSettings = nakadiSettings;
        this.metricRegistry = metricRegistry;
        this.zookeeperSettings = zookeeperSettings;
        this.kafkaSettings = kafkaSettings;
        this.uuidGenerator = uuidGenerator;
    }

    public StorageWorker getWorker(final Storage storage) {
        if (storages.containsKey(storage.getId())) {
            return storages.get(storage.getId());
        }
        synchronized (this) {
            if (storages.containsKey(storage.getId())) {
                return storages.get(storage.getId());
            }
            final StorageWorker storageWorker;
            switch (storage.getType()) {
                case KAFKA:
                    try {
                        storageWorker = createKafkaStorage((Storage.KafkaStorage) storage);
                    } catch (final RuntimeException ex) {
                        throw ex;
                    } catch (final Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    break;
                default:
                    throw new RuntimeException("Storage type " + storage.getType() + " is not supported");
            }
            storages.put(storage.getId(), storageWorker);
            return storageWorker;
        }
    }

    private StorageWorker createKafkaStorage(final Storage.KafkaStorage storage) throws Exception {
        return new KafkaStorageWorker(storage, metricRegistry, kafkaPartitionsCalculator, nakadiSettings, zookeeperSettings, kafkaSettings, uuidGenerator);
    }
}
