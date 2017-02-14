package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryFactory;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.StorageService;
import org.zalando.nakadi.view.TimelineRequest;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
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

    public TimelineService(final SecuritySettings securitySettings,
                           final EventTypeCache eventTypeCache,
                           final StorageService storageService,
                           final TimelineSync timelineSync,
                           final NakadiSettings nakadiSettings,
                           final TimelineDbRepository timelineDbRepository) {
        this.securitySettings = securitySettings;
        this.eventTypeCache = eventTypeCache;
        this.storageService = storageService;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.timelineDbRepository = timelineDbRepository;
    }

    public Result<?> createTimeline(final TimelineRequest timelineRequest, final Client client) {
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
                    TopicRepositoryFactory.createTopicRepository(activeTimeline.getStorage());
            final TopicRepository nextTopicRepo = TopicRepositoryFactory.createTopicRepository(storage);
            final List<PartitionStatistics> partitionStatistics =
                    currentTopicRepo.loadTopicStatistics(Collections.singleton(activeTimeline.getTopic()));

            if (activeTimeline.isFake()) {
                final Timeline nextTimeline = new Timeline(activeTimeline.getEventType(), activeTimeline.getOrder() + 1,
                        storage, activeTimeline.getTopic(), new Date());
                switchFakeTimeline(eventType.getName(), nextTimeline);
            } else {
                final String newTopic = nextTopicRepo.createTopic(partitionStatistics.size(),
                        eventType.getOptions().getRetentionTime());
                final Timeline nextTimeline = new Timeline(activeTimeline.getEventType(), activeTimeline.getOrder() + 1,
                        storage, newTopic, new Date());
                timelineDbRepository.createTimeline(nextTimeline);
                timelineSync.startTimelineUpdate(eventType.getName(), getTimelineUpdateTimeout());
                final Timeline.StoragePosition storagePosition =
                        StoragePositionFactory.createStoragePosition(activeTimeline, currentTopicRepo);
                activeTimeline.setLatestPosition(storagePosition);
                nextTimeline.setSwitchedAt(new Date());
                timelineDbRepository.updateTimelime(nextTimeline);
                timelineSync.finishTimelineUpdate(eventTypeName);
            }
            return Result.ok();
        } catch (final NakadiException ne) {
            return Result.problem(ne.asProblem());
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE));
        }
    }

    public Timeline getTimeline(final EventType eventType) throws InternalNakadiException, NoSuchEventTypeException {
        final String eventTypeName = eventType.getName();
        final Optional<Timeline> activeTimeline = eventTypeCache.getActiveTimeline(eventTypeName);
        if (activeTimeline.isPresent()) {
            return activeTimeline.get();
        }

        final Result<Storage> storageResult = storageService.getStorage(DEFAULT_STORAGE);
        if (storageResult.isSuccessful()) {
            return Timeline.createFakeTimeline(eventType, storageResult.getValue());
        }
        LOG.debug("Fake timeline creation failed for event type {}. No default storage defined", eventType.getName());
        throw new InternalNakadiException("No default storage defined");
    }

    private long getTimelineUpdateTimeout() {
        return (long) (nakadiSettings.getTimelineWaitTimeoutMs() * 0.9);
    }

    private void switchFakeTimeline(final String eventTypeName, final Timeline nextTimeline)
            throws InterruptedException {
        timelineDbRepository.createTimeline(nextTimeline);
        timelineSync.startTimelineUpdate(eventTypeName, getTimelineUpdateTimeout());
        nextTimeline.setSwitchedAt(new Date());
        timelineDbRepository.updateTimelime(nextTimeline);
        timelineSync.finishTimelineUpdate(eventTypeName);
    }

}
