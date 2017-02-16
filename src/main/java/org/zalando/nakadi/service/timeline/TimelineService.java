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
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.exceptions.TopicRepositoryException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.view.TimelineRequest;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Service
public class TimelineService {

    private static final Logger LOG = LoggerFactory.getLogger(TimelineService.class);
    private static final String DEFAULT_STORAGE = "default";

    private final SecuritySettings securitySettings;
    private final EventTypeCache eventTypeCache;
    private final StorageDbRepository storageDbRepository;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final TimelineDbRepository timelineDbRepository;
    private final TopicRepositoryHolder repositoryHolder;

    @Autowired
    public TimelineService(final SecuritySettings securitySettings,
                           final EventTypeCache eventTypeCache,
                           final StorageDbRepository storageDbRepository,
                           final TimelineSync timelineSync,
                           final NakadiSettings nakadiSettings,
                           final TimelineDbRepository timelineDbRepository,
                           final TopicRepositoryHolder repositoryHolder) {
        this.securitySettings = securitySettings;
        this.eventTypeCache = eventTypeCache;
        this.storageDbRepository = storageDbRepository;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.timelineDbRepository = timelineDbRepository;
        this.repositoryHolder = repositoryHolder;
    }

    public void createTimeline(final TimelineRequest timelineRequest, final Client client)
            throws ForbiddenAccessException, TimelineException, TopicRepositoryException {
        if (!client.getClientId().equals(securitySettings.getAdminClientId())) {
            throw new ForbiddenAccessException("Request is forbidden for user " + client.getClientId());
        }

        try {
            final String eventTypeName = timelineRequest.getEventType();
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);

            final Storage storage = storageDbRepository.getStorage(timelineRequest.getStorageId())
                    .orElseThrow(() -> new NotFoundException("No storage with id: " + timelineRequest.getStorageId()));
            final Timeline activeTimeline = getTimeline(eventType);
            final TopicRepository currentTopicRepo = repositoryHolder.getTopicRepository(activeTimeline.getStorage());
            final List<PartitionStatistics> partitionStatistics =
                    currentTopicRepo.loadTopicStatistics(Collections.singleton(activeTimeline.getTopic()));

            if (activeTimeline.isFake()) {
                LOG.info("Switching from fake timeline to real one. Current topic {}", activeTimeline.getTopic());
                switchTimeline(eventType, Timeline.createTimeline(activeTimeline.getEventType(),
                        activeTimeline.getOrder() + 1, storage, activeTimeline.getTopic(), new Date()),
                        (nextTimeline) -> nextTimeline.setSwitchedAt(new Date()));
            } else {
                final TopicRepository nextTopicRepo = repositoryHolder.getTopicRepository(storage);
                final String newTopic = nextTopicRepo.createTopic(partitionStatistics.size(),
                        eventType.getOptions().getRetentionTime());
                LOG.info("Switching timelines. Topics: {} -> {}", activeTimeline.getTopic(), newTopic);
                switchTimeline(eventType, Timeline.createTimeline(activeTimeline.getEventType(),
                        activeTimeline.getOrder() + 1, storage, newTopic, new Date()), (nextTimeline) -> {
                    activeTimeline.setLatestPosition(repositoryHolder.createStoragePosition(activeTimeline));
                    nextTimeline.setSwitchedAt(new Date());
                });
            }
        } catch (final NakadiException ne) {
            throw new TimelineException(ne.getMessage(), ne);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TimelineException(
                    String.format("Timeline update was interrupted for event type %s and storage id `%s`",
                            timelineRequest.getEventType(), timelineRequest.getStorageId()));
        }
    }

    public Timeline getTimeline(final EventType eventType) throws TimelineException {
        try {
            final String eventTypeName = eventType.getName();
            final Optional<Timeline> activeTimeline = eventTypeCache.getActiveTimeline(eventTypeName);
            if (activeTimeline.isPresent()) {
                return activeTimeline.get();
            }

            final Storage storage = storageDbRepository.getStorage(DEFAULT_STORAGE)
                    .orElseThrow(() -> new NotFoundException("Fake timeline creation failed for event type " +
                            eventType.getName() + ".No default storage defined"));
            return Timeline.createFakeTimeline(eventType, storage);
        } catch (final NakadiException e) {
            LOG.error("Failed to get timeline for event type {}", eventType.getName(), e);
            throw new TimelineException("Failed to get timeline", e);
        }
    }

    public TopicRepository getTopicRepository(final EventType eventType)
            throws TopicRepositoryException, TimelineException {
        final Timeline timeline = getTimeline(eventType);
        return repositoryHolder.getTopicRepository(timeline.getStorage());
    }

    private void switchTimeline(final EventType eventType,
                                final Timeline nextTimeline,
                                final Consumer<Timeline> switcher) throws InterruptedException {
        try {
            timelineDbRepository.createTimeline(nextTimeline);
            timelineSync.startTimelineUpdate(eventType.getName(), nakadiSettings.getTimelineWaitTimeoutMs());
            switcher.accept(nextTimeline);
            timelineDbRepository.updateTimelime(nextTimeline);
        } catch (final Throwable th) {
            LOG.error("Failed to switch timeline for event type {}", eventType.getName(), th);
            timelineDbRepository.deleteTimeline(nextTimeline.getId());
            throw th;
        } finally {
            timelineSync.finishTimelineUpdate(eventType.getName());
        }
    }
}
