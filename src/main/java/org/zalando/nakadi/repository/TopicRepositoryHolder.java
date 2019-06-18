package org.zalando.nakadi.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Component
public class TopicRepositoryHolder {

    private static final Logger LOG = LoggerFactory.getLogger(TopicRepositoryHolder.class);

    private final Map<Storage.Type, TopicRepositoryCreator> repositoryCreators;
    private final Map<Storage, TopicRepository> storageTopicRepository;

    private final Lock lock = new ReentrantLock();
    private final Condition loadingListChanged = lock.newCondition();
    private final Set<Storage> storagesBeingLoaded = new HashSet<>();

    @Autowired
    public TopicRepositoryHolder(@Qualifier("kafka") final TopicRepositoryCreator kafkaRepository) {

        this.storageTopicRepository = new HashMap<>();
        this.repositoryCreators = new HashMap<>();
        this.repositoryCreators.put(kafkaRepository.getSupportedStorageType(), kafkaRepository);
    }

    public TopicRepository getTopicRepository(final Storage storage) throws TopicRepositoryException {
        lock.lock();
        try {
            TopicRepository topicRepository = storageTopicRepository.get(storage);
            if (topicRepository != null) {
                return topicRepository;
            }
            while (storagesBeingLoaded.contains(storage)) {
                loadingListChanged.await();
            }
            topicRepository = storageTopicRepository.get(storage);
            if (null != topicRepository) {
                return topicRepository;
            }
            storagesBeingLoaded.add(storage);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new TopicRepositoryException("Interrupted while waiting for topic repository creation", ex);
        } finally {
            lock.unlock();
        }

        TopicRepository created = null;
        try {
            created = getTopicRepositoryCreator(storage.getType()).createTopicRepository(storage);
            return created;
        } finally {
            lock.lock();
            try {
                if (null != created) {
                    storageTopicRepository.put(storage, created);
                }
                storagesBeingLoaded.remove(storage);
                loadingListChanged.signalAll();
            } finally {
                lock.unlock();
            }
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
        } catch (final ServiceTemporarilyUnavailableException e) {
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
