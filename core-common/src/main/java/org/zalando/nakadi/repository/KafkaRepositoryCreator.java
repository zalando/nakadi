package org.zalando.nakadi.repository;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.repository.kafka.KafkaLocationManager;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.kafka.KafkaTopicConfigFactory;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaZookeeper;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component(value = "kafka")
public class KafkaRepositoryCreator implements TopicRepositoryCreator {

    private final NakadiSettings nakadiSettings;
    private final KafkaSettings kafkaSettings;
    private final ZookeeperSettings zookeeperSettings;
    private final KafkaTopicConfigFactory kafkaTopicConfigFactory;
    private final MetricRegistry metricRegistry;
    private final ObjectMapper objectMapper;
    private final NakadiRecordMapper nakadiRecordMapper;

    @Autowired
    public KafkaRepositoryCreator(
            final NakadiSettings nakadiSettings,
            final KafkaSettings kafkaSettings,
            final ZookeeperSettings zookeeperSettings,
            final KafkaTopicConfigFactory kafkaTopicConfigFactory,
            final MetricRegistry metricRegistry,
            final ObjectMapper objectMapper,
            final NakadiRecordMapper nakadiRecordMapper) {
        this.nakadiSettings = nakadiSettings;
        this.kafkaSettings = kafkaSettings;
        this.zookeeperSettings = zookeeperSettings;
        this.kafkaTopicConfigFactory = kafkaTopicConfigFactory;
        this.metricRegistry = metricRegistry;
        this.objectMapper = objectMapper;
        this.nakadiRecordMapper = nakadiRecordMapper;
    }

    @Override
    public TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
        try {
            final KafkaConfiguration kafkaConfiguration = storage.getKafkaConfiguration();
            final ZooKeeperHolder zooKeeperHolder = new ZooKeeperHolder(
                    kafkaConfiguration.getZookeeperConnection(),
                    zookeeperSettings.getZkSessionTimeoutMs(),
                    zookeeperSettings.getZkConnectionTimeoutMs(),
                    nakadiSettings);
            final KafkaLocationManager kafkaLocationManager = new KafkaLocationManager(zooKeeperHolder, kafkaSettings);
            final KafkaFactory kafkaFactory = new KafkaFactory(kafkaLocationManager);
            final KafkaZookeeper zk = new KafkaZookeeper(zooKeeperHolder, objectMapper);
            final KafkaTopicRepository kafkaTopicRepository =
                    new KafkaTopicRepository.Builder()
                            .setKafkaZookeeper(zk)
                            .setKafkaFactory(kafkaFactory)
                            .setNakadiSettings(nakadiSettings)
                            .setKafkaSettings(kafkaSettings)
                            .setKafkaTopicConfigFactory(kafkaTopicConfigFactory)
                            .setKafkaLocationManager(kafkaLocationManager)
                            .setNakadiRecordMapper(nakadiRecordMapper)
                            .build();
            // check that it does work
            kafkaTopicRepository.listTopics();
            return kafkaTopicRepository;
        } catch (final Exception e) {
            throw new TopicRepositoryException("Could not create topic repository", e);
        }
    }

    @Override
    public Timeline.StoragePosition createStoragePosition(final List<NakadiCursor> offsets)
            throws NakadiRuntimeException {
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
