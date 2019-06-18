package org.zalando.nakadi.config;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.zalando.nakadi.domain.storage.DefaultStorage;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.StorageService;

import java.util.Optional;

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
    public DefaultStorage defaultStorage(final StorageDbRepository storageDbRepository,
                                         final Environment environment,
                                         final ZooKeeperHolder zooKeeperHolder)
            throws InternalNakadiException {
        final String storageId = getStorageId(zooKeeperHolder, environment);
        final Optional<Storage> storageOpt = storageDbRepository.getStorage(storageId);
        if (!storageOpt.isPresent()) {
            LOGGER.info("Creating timelines storage `{}` from defaults", storageId);
            final Storage storage = new Storage();
            storage.setId(storageId);
            storage.setType(Storage.Type.KAFKA);
            storage.setConfiguration(new KafkaConfiguration(
                    ZookeeperConnection.valueOf(environment.getProperty("nakadi.zookeeper.connectionString"))));
            try {
                storageDbRepository.createStorage(storage);
            } catch (final DuplicatedStorageException e) {
                LOGGER.info("Creation of default storage failed: {}", e.getMessage());
            }
            return new DefaultStorage(storage);
        } else {
            return new DefaultStorage(storageOpt.get());
        }
    }

    private String getStorageId(final ZooKeeperHolder zooKeeperHolder,
                                final Environment environment) {
        final CuratorFramework curator = zooKeeperHolder.get();
        try {
            curator.create().creatingParentsIfNeeded()
                    .forPath(StorageService.ZK_TIMELINES_DEFAULT_STORAGE, null);
        } catch (final KeeperException.NodeExistsException e) {
            LOGGER.trace("Node {} already is there", StorageService.ZK_TIMELINES_DEFAULT_STORAGE);
        } catch (final Exception e) {
            LOGGER.error("Zookeeper access error {}", e.getMessage(), e);
        }

        try {
            final byte[] storageIdBytes = curator.getData()
                    .forPath(StorageService.ZK_TIMELINES_DEFAULT_STORAGE);
            if (storageIdBytes != null) {
                return new String(storageIdBytes, Charsets.UTF_8);
            }
        } catch (final Exception e) {
            LOGGER.warn("Init of default storage from zk failed, will use default from env", e);
        }

        return environment.getProperty("nakadi.timelines.storage.default");
    }

}
