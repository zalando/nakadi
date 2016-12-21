package org.zalando.nakadi.service.timeline;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.repository.kafka.KafkaPartitionsCalculator;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageWorkerFactory {

    private final ConcurrentHashMap<String, StorageWorker> storages = new ConcurrentHashMap<>();
    private final KafkaPartitionsCalculator kafkaPartitionsCalculator;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public StorageWorkerFactory(final KafkaPartitionsCalculator kafkaPartitionsCalculator,
                                final NakadiSettings nakadiSettings) {
        this.kafkaPartitionsCalculator = kafkaPartitionsCalculator;
        this.nakadiSettings = nakadiSettings;
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
                    storageWorker = createKafkaStorage((Storage.KafkaStorage) storage);
                    break;
                default:
                    throw new RuntimeException("Storage type " + storage.getType() + " is not supported");
            }
            storages.put(storage.getId(), storageWorker);
            return storageWorker;
        }
    }

    private StorageWorker createKafkaStorage(final Storage.KafkaStorage storage) {
        return new KafkaStorageWorker(storage, kafkaPartitionsCalculator, nakadiSettings);
    }
}
