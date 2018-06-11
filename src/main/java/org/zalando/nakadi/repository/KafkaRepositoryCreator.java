package org.zalando.nakadi.repository;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiWrapperException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.repository.kafka.KafkaLocationManager;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component(value = "kafka")
public class KafkaRepositoryCreator implements TopicRepositoryCreator {

    private final NakadiSettings nakadiSettings;
    private final KafkaSettings kafkaSettings;
    private final ZookeeperSettings zookeeperSettings;
    private final UUIDGenerator uuidGenerator;
    private final MetricRegistry metricRegistry;

    @Autowired
    public KafkaRepositoryCreator(final NakadiSettings nakadiSettings,
                                  final KafkaSettings kafkaSettings,
                                  final ZookeeperSettings zookeeperSettings,
                                  final UUIDGenerator uuidGenerator,
                                  final MetricRegistry metricRegistry) {
        this.nakadiSettings = nakadiSettings;
        this.kafkaSettings = kafkaSettings;
        this.zookeeperSettings = zookeeperSettings;
        this.uuidGenerator = uuidGenerator;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
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
            // check that it does work
            kafkaTopicRepository.listTopics();
            return kafkaTopicRepository;
        } catch (final Exception e) {
            throw new TopicRepositoryException("Could not create topic repository", e);
        }
    }

    @Override
    public Timeline.StoragePosition createStoragePosition(final List<NakadiCursor> offsets)
            throws NakadiWrapperException {
        final Timeline.KafkaStoragePosition kafkaStoragePosition = new Timeline.KafkaStoragePosition();
        kafkaStoragePosition.setOffsets(offsets.stream()
                .sorted(Comparator.comparing(p -> Integer.valueOf(p.getPartition())))
                .map(nakadiCursor -> Long.valueOf(nakadiCursor.getOffset()))
                .collect(Collectors.toList()));
        return kafkaStoragePosition;
    }

    @Override
    public Storage.Type getSupportedStorageType() {
        return Storage.Type.KAFKA;
    }

}
