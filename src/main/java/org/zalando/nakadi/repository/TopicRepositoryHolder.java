package org.zalando.nakadi.repository;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.TopicRepositoryException;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.repository.kafka.KafkaLocationManager;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class TopicRepositoryHolder {

    private static final Logger LOG = LoggerFactory.getLogger(TopicRepositoryHolder.class);
    private final NakadiSettings nakadiSettings;
    private final KafkaSettings kafkaSettings;
    private final ZookeeperSettings zookeeperSettings;
    private final UUIDGenerator uuidGenerator;
    private final MetricRegistry metricRegistry;
    private final ReadWriteLock readWriteLock;
    private final Map<Storage, TopicRepository> storageTopicRepository;

    @Autowired
    public TopicRepositoryHolder(final NakadiSettings nakadiSettings,
                                 final KafkaSettings kafkaSettings,
                                 final ZookeeperSettings zookeeperSettings,
                                 final UUIDGenerator uuidGenerator,
                                 final MetricRegistry metricRegistry) {
        this.nakadiSettings = nakadiSettings;
        this.kafkaSettings = kafkaSettings;
        this.zookeeperSettings = zookeeperSettings;
        this.uuidGenerator = uuidGenerator;
        this.metricRegistry = metricRegistry;
        this.readWriteLock = new ReentrantReadWriteLock();
        this.storageTopicRepository = new HashMap<>();
    }

    public TopicRepository getTopicRepository(final Storage storage) throws TopicRepositoryException {
        readWriteLock.readLock().lock();
        try {
            final TopicRepository topicRepository = storageTopicRepository.get(storage);
            if (topicRepository != null) {
                return topicRepository;
            }
        } finally {
            readWriteLock.readLock().unlock();
        }

        readWriteLock.writeLock().lock();
        try {
            TopicRepository topicRepository = storageTopicRepository.get(storage);
            if (topicRepository != null) {
                return topicRepository;
            }
            topicRepository = createTopicRepository(storage);
            storageTopicRepository.put(storage, topicRepository);
            return topicRepository;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
        switch (storage.getType()) {
            case KAFKA:
                try {
                    final Storage.KafkaConfiguration kafkaConfiguration = storage.getKafkaConfiguration();
                    final ZooKeeperHolder zooKeeperHolder = new ZooKeeperHolder(
                            kafkaConfiguration.getZkAddress(),
                            kafkaConfiguration.getZkPath(),
                            kafkaConfiguration.getExhibitorAddress(),
                            kafkaConfiguration.getExhibitorPort());
                    final KafkaFactory kafkaFactory =
                            new KafkaFactory(new KafkaLocationManager(zooKeeperHolder, kafkaSettings), metricRegistry);
                    final KafkaTopicRepository kafkaTopicRepository = new KafkaTopicRepository(zooKeeperHolder,
                            kafkaFactory, nakadiSettings, kafkaSettings, zookeeperSettings, uuidGenerator);
                    return kafkaTopicRepository;
                } catch (final Exception e) {
                    LOG.error("Could not create topic repository", storage.getType(), e);
                    throw new TopicRepositoryException("Could not create topic repository", e);
                }
            default:
                LOG.error("Could not create topic repository. Storage type {} not found", storage.getType());
                throw new TopicRepositoryException("Could not create topic repository");
        }
    }
}
