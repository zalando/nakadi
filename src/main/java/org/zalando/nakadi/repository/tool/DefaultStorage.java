package org.zalando.nakadi.repository.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.DuplicatedStorageIdException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.repository.db.StorageDbRepository;

@Service
public class DefaultStorage implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorage.class);
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
    public void run(final ApplicationArguments args) throws InternalNakadiException {
        final Storage storage = new Storage();
        storage.setId(DEFAULT_STORAGE);
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(new Storage.KafkaConfiguration(
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.valueOf(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0")),
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", "")));
        try {
            storageDbRepository.createStorage(storage);
        } catch (final DuplicatedStorageIdException e) {
            LOG.info("Creation of default storage failed: {}", e.getMessage());
        }
    }
}
