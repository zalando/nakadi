package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.exceptions.TopicRepositoryException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.StorageService;
import org.zalando.nakadi.view.TimelineRequest;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
public class TimelineService {

    private static final Logger LOG = LoggerFactory.getLogger(TimelineService.class);
    private static final String DEFAULT_STORAGE = "default";

    private final SecuritySettings securitySettings;
    private final EventTypeCache eventTypeCache;
    private final StorageService storageService;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final TimelineDbRepository timelineDbRepository;
    private final TopicRepositoryHolder topicRepositoryHolder;

    @Autowired
    public TimelineService(final SecuritySettings securitySettings,
                           final EventTypeCache eventTypeCache,
                           final StorageService storageService,
                           final TimelineSync timelineSync,
                           final NakadiSettings nakadiSettings,
                           final TimelineDbRepository timelineDbRepository,
                           final TopicRepositoryHolder topicRepositoryHolder) {
        this.securitySettings = securitySettings;
        this.eventTypeCache = eventTypeCache;
        this.storageService = storageService;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.timelineDbRepository = timelineDbRepository;
        this.topicRepositoryHolder = topicRepositoryHolder;
    }

    public void createTimeline(final TimelineRequest timelineRequest, final Client client)
            throws ForbiddenAccessException, TimelineException, TopicRepositoryException {
        if (!client.getClientId().equals(securitySettings.getAdminClientId())) {
            throw new ForbiddenAccessException();
        }

        try {
            final String eventTypeName = timelineRequest.getEventType();
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);

            final Result<Storage> storageResult = storageService.getStorage(timelineRequest.getStorageId());
            if (storageResult.isSuccessful()) {
                throw new RuntimeException();
            }

            final Storage storage = storageResult.getValue();
            final Timeline activeTimeline = getTimeline(eventType);
            final TopicRepository currentTopicRepo =
                    topicRepositoryHolder.getTopicRepository(activeTimeline.getStorage());
            final TopicRepository nextTopicRepo = topicRepositoryHolder.getTopicRepository(storage);
            final List<PartitionStatistics> partitionStatistics =
                    currentTopicRepo.loadTopicStatistics(Collections.singleton(activeTimeline.getTopic()));

            if (activeTimeline.isFake()) {
                final Timeline nextTimeline = new Timeline(activeTimeline.getEventType(), activeTimeline.getOrder() + 1,
                        storage, activeTimeline.getTopic(), new Date());
                switchTimeline(eventType, nextTimeline, () -> nextTimeline.setSwitchedAt(new Date()));
            } else {
                final String newTopic = nextTopicRepo.createTopic(partitionStatistics.size(),
                        eventType.getOptions().getRetentionTime());
                final Timeline nextTimeline = new Timeline(activeTimeline.getEventType(), activeTimeline.getOrder() + 1,
                        storage, newTopic, new Date());
                switchTimeline(eventType, nextTimeline, () -> {
                    final Timeline.StoragePosition storagePosition =
                            StoragePositionFactory.createStoragePosition(activeTimeline, currentTopicRepo);
                    activeTimeline.setLatestPosition(storagePosition);
                    nextTimeline.setSwitchedAt(new Date());
                });
            }
        } catch (final NakadiException ne) {
            LOG.error(ne.getMessage(), ne);
            throw new TimelineException(ne.getMessage(), ne);
        } catch (final InterruptedException ie) {
            LOG.error("Timeline update was interrupted for event type {} and storage {}",
                    timelineRequest.getEventType(), timelineRequest.getStorageId());
            Thread.currentThread().interrupt();
            throw new TimelineException("Timeline update was interrupted");
        }
    }

    public Timeline getTimeline(final EventType eventType) throws TimelineException {
        try {
            final String eventTypeName = eventType.getName();
            final Optional<Timeline> activeTimeline = eventTypeCache.getActiveTimeline(eventTypeName);
            if (activeTimeline.isPresent()) {
                return activeTimeline.get();
            }
        } catch (final NakadiException e) {
            LOG.error("Failed to get timeline for event type {}", eventType.getName(), e);
            throw new TimelineException("Failed to get timeline", e);
        }

        final Result<Storage> storageResult = storageService.getStorage(DEFAULT_STORAGE);
        if (storageResult.isSuccessful()) {
            return Timeline.createFakeTimeline(eventType, storageResult.getValue());
        }

        LOG.error("Fake timeline creation failed for event type {}. No default storage defined", eventType.getName());
        throw new TimelineException("No default storage found");
    }

    public TopicRepository getTopicRepository(final EventType eventType)
            throws TopicRepositoryException, TimelineException {
        final Timeline timeline = getTimeline(eventType);
        return topicRepositoryHolder.getTopicRepository(timeline.getStorage());
    }

    private long getTimelineUpdateTimeout() {
        return (long) (nakadiSettings.getTimelineWaitTimeoutMs() * 0.9);
    }

    private void switchTimeline(final EventType eventType,
                                final Timeline nextTimeline,
                                final Runnable switcher) throws InterruptedException {
        try {
            timelineDbRepository.createTimeline(nextTimeline);
            timelineSync.startTimelineUpdate(eventType.getName(), getTimelineUpdateTimeout());
            switcher.run();
            timelineDbRepository.updateTimelime(nextTimeline);
        } finally {
            timelineSync.finishTimelineUpdate(eventType.getName());
        }
    }
}
