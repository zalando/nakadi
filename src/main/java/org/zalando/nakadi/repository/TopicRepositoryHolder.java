package org.zalando.nakadi.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Component
public class TopicRepositoryHolder {

    private static final Logger LOG = LoggerFactory.getLogger(TopicRepositoryHolder.class);

    private final Map<Storage.Type, TopicRepositoryCreator> repositoryCreators;
    private final ReadWriteLock readWriteLock;
    private final Map<Storage, TopicRepository> storageTopicRepository;

    @Autowired
    public TopicRepositoryHolder(@Qualifier("kafka") final TopicRepositoryCreator kafkaRepository) {
        this.readWriteLock = new ReentrantReadWriteLock();
        this.storageTopicRepository = new HashMap<>();
        this.repositoryCreators = new HashMap<>();
        this.repositoryCreators.put(kafkaRepository.getSupportedStorageType(), kafkaRepository);
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

            topicRepository = getTopicRepositoryCreator(storage.getType()).createTopicRepository(storage);
            storageTopicRepository.put(storage, topicRepository);
            return topicRepository;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public Timeline.StoragePosition createStoragePosition(final Timeline timeline) {
        try {
            final Storage storage = timeline.getStorage();
            final List<NakadiCursor> offsets = getTopicRepository(storage)
                    .loadTopicStatistics(Collections.singleton(timeline)).stream()
                    .map(PartitionStatistics::getLast)
                    .collect(Collectors.toList());
            return getTopicRepositoryCreator(storage.getType()).createStoragePosition(offsets);
        } catch (final ServiceUnavailableException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private TopicRepositoryCreator getTopicRepositoryCreator(final Storage.Type type) {
        final TopicRepositoryCreator topicRepositoryCreator = repositoryCreators.get(type);
        if (topicRepositoryCreator == null) {
            throw new TopicRepositoryException("Could not create topic repository. Storage type not found: " + type);
        }
        return topicRepositoryCreator;
    }

}
