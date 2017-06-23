package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;

@Configuration
@EnableScheduling
public class NakadiConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(NakadiConfig.class);

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ZooKeeperLockFactory zooKeeperLockFactory(final ZooKeeperHolder zooKeeperHolder) {
        return new ZooKeeperLockFactory(zooKeeperHolder);
    }

    @Bean
    @Qualifier("default_storage")
    public Storage defaultStorage(final StorageDbRepository storageDbRepository,
                                  final Environment environment) throws InternalNakadiException {
        final Storage storage = new Storage();
        storage.setId("default");
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(new Storage.KafkaConfiguration(
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.valueOf(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0")),
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", "")));
        try {
            storageDbRepository.createStorage(storage);
        } catch (final DuplicatedStorageException e) {
            LOGGER.info("Creation of default storage failed: {}", e.getMessage());
        }
        return storage;
    }

}
