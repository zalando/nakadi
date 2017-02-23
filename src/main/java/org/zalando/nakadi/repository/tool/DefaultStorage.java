package org.zalando.nakadi.repository.tool;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.repository.db.StorageDbRepository;

import java.util.Optional;

@Service
public class DefaultStorage implements ApplicationRunner {

    private static final String DEFAULT_STORAGE = "default";
    private final Environment environment;
    private final StorageDbRepository storageDbRepository;

    @Autowired
    public DefaultStorage(final StorageDbRepository storageDbRepository,
                          final Environment environment) {
        this.storageDbRepository = storageDbRepository;
        this.environment = environment;
    }

    @Override
    public void run(final ApplicationArguments args) throws Exception {
        final Optional<Storage> storageOpt = storageDbRepository.getStorage(DEFAULT_STORAGE);
        if (!storageOpt.isPresent()) {
            final Storage storage = new Storage();
            storage.setId(DEFAULT_STORAGE);
            storage.setType(Storage.Type.KAFKA);
            storage.setConfiguration(new Storage.KafkaConfiguration(
                    environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                    Integer.valueOf(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0")),
                    environment.getProperty("nakadi.zookeeper.brokers"),
                    environment.getProperty("nakadi.zookeeper.kafkaNamespace", "")));
            storageDbRepository.createStorage(storage);
        }
    }
}
