package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.DefaultStorage;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeResource;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.ConflictException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedTimelineException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.MultiTimelineEventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.NakadiCursorComparator;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

@Service
public class TimelineService {

    private static final Logger LOG = LoggerFactory.getLogger(TimelineService.class);

    private final EventTypeCache eventTypeCache;
    private final StorageDbRepository storageDbRepository;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final TimelineDbRepository timelineDbRepository;
    private final TopicRepositoryHolder topicRepositoryHolder;
    private final TransactionTemplate transactionTemplate;
    private final DefaultStorage defaultStorage;
    private final AdminService adminService;

    @Autowired
    public TimelineService(final EventTypeCache eventTypeCache,
                           final StorageDbRepository storageDbRepository,
                           final TimelineSync timelineSync,
                           final NakadiSettings nakadiSettings,
                           final TimelineDbRepository timelineDbRepository,
                           final TopicRepositoryHolder topicRepositoryHolder,
                           final TransactionTemplate transactionTemplate,
                           @Qualifier("default_storage") final DefaultStorage defaultStorage,
                           final AdminService adminService) {
        this.eventTypeCache = eventTypeCache;
        this.storageDbRepository = storageDbRepository;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.timelineDbRepository = timelineDbRepository;
        this.topicRepositoryHolder = topicRepositoryHolder;
        this.transactionTemplate = transactionTemplate;
        this.defaultStorage = defaultStorage;
        this.adminService = adminService;
    }

    public void createTimeline(final String eventTypeName, final String storageId)
            throws AccessDeniedException, TimelineException, TopicRepositoryException, InconsistentStateException,
            RepositoryProblemException {
        try {
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);

            if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
                final Resource resource = new EventTypeResource(eventTypeName, eventType.getAuthorization());
                throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource);
            }

            final Storage storage = storageDbRepository.getStorage(storageId)
                    .orElseThrow(() -> new UnableProcessException("No storage with id: " + storageId));
            final Timeline activeTimeline = getActiveTimeline(eventType);
            final TopicRepository currentTopicRepo =
                    topicRepositoryHolder.getTopicRepository(activeTimeline.getStorage());
            final TopicRepository nextTopicRepo = topicRepositoryHolder.getTopicRepository(storage);
            final List<PartitionStatistics> partitionStatistics =
                    currentTopicRepo.loadTopicStatistics(Collections.singleton(activeTimeline));

            final String newTopic = nextTopicRepo.createTopic(partitionStatistics.size(),
                    eventType.getOptions().getRetentionTime());
            final Timeline nextTimeline = Timeline.createTimeline(activeTimeline.getEventType(),
                    activeTimeline.getOrder() + 1, storage, newTopic, new Date());

            switchTimelines(activeTimeline, nextTimeline);
        } catch (final TopicCreationException | ServiceUnavailableException | InternalNakadiException e) {
            throw new TimelineException("Internal service error", e);
        } catch (final NoSuchEventTypeException e) {
            throw new NotFoundException("EventType \"" + eventTypeName + "\" does not exist", e);
        }
    }

    public Timeline createDefaultTimeline(final String eventTypeName,
                                          final int partitionsCount,
                                          final long retentionTime)
            throws TopicCreationException,
            InconsistentStateException,
            RepositoryProblemException,
            DuplicatedTimelineException,
            TimelineException {
        final TopicRepository repository = topicRepositoryHolder.getTopicRepository(defaultStorage.getStorage());
        final String topic = repository.createTopic(partitionsCount, retentionTime);

        try {
            final Timeline timeline = Timeline.createTimeline(eventTypeName, 1,
                    defaultStorage.getStorage(), topic, new Date());
            timeline.setSwitchedAt(new Date());
            timelineDbRepository.createTimeline(timeline);
            eventTypeCache.updated(eventTypeName);
            return timeline;
        } catch (final InconsistentStateException | RepositoryProblemException | DuplicatedTimelineException e) {
            rollbackTopic(repository, topic);
            throw e;
        } catch (final Exception e) {
            rollbackTopic(repository, topic);
            throw new TimelineException("Failed to update event type cache, while creating timeline", e);
        }
    }

    private void rollbackTopic(final TopicRepository repository, final String topic) {
        try {
            repository.deleteTopic(topic);
        } catch (final TopicDeletionException ex) {
            LOG.error("Failed to delete topic while recovering from timeline creation failure: {}",
                    ex.getProblemMessage());
        }
    }

    /**
     * Returns list of ACTIVE timelines for event type.
     *
     * @param eventType
     * @return list of active timelines
     * @throws InternalNakadiException  everything can happen
     * @throws NoSuchEventTypeException No such event type
     */
    public List<Timeline> getActiveTimelinesOrdered(final String eventType)
            throws InternalNakadiException, NoSuchEventTypeException {
        final List<Timeline> timelines = eventTypeCache.getTimelinesOrdered(eventType);
        return timelines.stream()
                .filter(t -> t.getSwitchedAt() != null && !t.isDeleted())
                .collect(Collectors.toList());
    }

    public List<Timeline> getAllTimelinesOrdered(final String eventType)
            throws InternalNakadiException, NoSuchEventTypeException {
        return eventTypeCache.getTimelinesOrdered(eventType);
    }

    public Timeline getActiveTimeline(final EventTypeBase eventType) throws TimelineException {
        try {
            final String eventTypeName = eventType.getName();
            final List<Timeline> timelines = eventTypeCache.getTimelinesOrdered(eventTypeName);
            final ListIterator<Timeline> rIterator = timelines.listIterator(timelines.size());
            while (rIterator.hasPrevious()) {
                final Timeline toCheck = rIterator.previous();
                if (toCheck.getSwitchedAt() != null) {
                    return toCheck;
                }
            }

            throw new TimelineException(String.format("No timelines for event type %s", eventTypeName));
        } catch (final NakadiException e) {
            LOG.error("Failed to get timeline for event type {}: {}", eventType.getName(), e.getProblemMessage());
            throw new TimelineException("Failed to get timeline", e);
        }
    }

    public TopicRepository getTopicRepository(final EventTypeBase eventType)
            throws TopicRepositoryException, TimelineException {
        final Timeline timeline = getActiveTimeline(eventType);
        return topicRepositoryHolder.getTopicRepository(timeline.getStorage());
    }

    public TopicRepository getTopicRepository(final Timeline timeline)
            throws TopicRepositoryException, TimelineException {
        return topicRepositoryHolder.getTopicRepository(timeline.getStorage());
    }

    public EventConsumer createEventConsumer(@Nullable final String clientId, final List<NakadiCursor> positions)
            throws NakadiException, InvalidCursorException {
        final MultiTimelineEventConsumer result = new MultiTimelineEventConsumer(
                clientId, this, timelineSync, new NakadiCursorComparator(eventTypeCache));
        result.reassign(positions);
        return result;
    }

    public EventConsumer.ReassignableEventConsumer createEventConsumer(@Nullable final String clientId) {
        return new MultiTimelineEventConsumer(
                clientId, this, timelineSync, new NakadiCursorComparator(eventTypeCache));
    }

    private void switchTimelines(final Timeline activeTimeline, final Timeline nextTimeline)
            throws InconsistentStateException, RepositoryProblemException, TimelineException, ConflictException {
        LOG.info("Switching timelines from {} to {}", activeTimeline, nextTimeline);
        try {
            timelineSync.startTimelineUpdate(activeTimeline.getEventType(), nakadiSettings.getTimelineWaitTimeoutMs());
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TimelineException("Failed to switch timeline for: " + activeTimeline.getEventType());
        } catch (final IllegalStateException ie) {
            throw new ConflictException("Timeline is already being created for: " + activeTimeline.getEventType(), ie);
        }

        try {
            transactionTemplate.execute(status -> {
                timelineDbRepository.createTimeline(nextTimeline);
                nextTimeline.setSwitchedAt(new Date());
                final Timeline.StoragePosition sp = topicRepositoryHolder.createStoragePosition(activeTimeline);
                activeTimeline.setLatestPosition(sp);
                scheduleTimelineCleanup(activeTimeline);
                timelineDbRepository.updateTimelime(activeTimeline);
                timelineDbRepository.updateTimelime(nextTimeline);
                return null;
            });
        } catch (final TransactionException tx) {
            LOG.error(tx.getMessage());
            throw new TimelineException("Failed to create timeline in DB for: " + activeTimeline.getEventType(), tx);
        } finally {
            finishTimelineUpdate(activeTimeline.getEventType());
        }
    }

    private void scheduleTimelineCleanup(final Timeline timeline) throws InconsistentStateException {
        try {
            final EventType eventType = eventTypeCache.getEventType(timeline.getEventType());
            final Long retentionTime = eventType.getOptions().getRetentionTime();
            if (retentionTime == null) {
                throw new InconsistentStateException("Event type should has information about its retention time");
            }
            final Date cleanupDate = new Date(System.currentTimeMillis() + retentionTime);
            timeline.setCleanedUpAt(cleanupDate);
        } catch (final InternalNakadiException | NoSuchEventTypeException e) {
            throw new InconsistentStateException("Unexpected error occurred when scheduling timeline cleanup", e);
        }
    }

    public Multimap<TopicRepository, String> deleteAllTimelinesForEventType(final String eventTypeName)
            throws TopicDeletionException,
            TimelineException,
            NotFoundException,
            InternalNakadiException,
            NoSuchEventTypeException {
        LOG.info("Deleting all timelines for event type {}", eventTypeName);
        final Multimap<TopicRepository, String> topicsToDelete = ArrayListMultimap.create();
        for (final Timeline timeline : getAllTimelinesOrdered(eventTypeName)) {
            topicsToDelete.put(getTopicRepository(timeline), timeline.getTopic());
            timelineDbRepository.deleteTimeline(timeline.getId());
        }
        return topicsToDelete;
    }

    private void finishTimelineUpdate(final String eventTypeName) throws TimelineException {
        try {
            timelineSync.finishTimelineUpdate(eventTypeName);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TimelineException("Timeline update was interrupted for:" + eventTypeName);
        } catch (final RuntimeException re) {
            throw new TimelineException("Failed to finish timeline update for:" + eventTypeName, re);
        }
    }

    public List<Timeline> getTimelines(final String eventTypeName)
            throws AccessDeniedException, UnableProcessException, TimelineException, NotFoundException {
        if (!adminService.isAdmin(AuthorizationService.Operation.READ)) {
            final Resource resource = new EventTypeResource(eventTypeName, null);
            throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource);
        }

        try {
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            return timelineDbRepository.listTimelinesOrdered(eventType.getName());
        } catch (final NoSuchEventTypeException e) {
            throw new NotFoundException("EventType \"" + eventTypeName + "\" does not exist", e);
        } catch (final InternalNakadiException e) {
            throw new TimelineException("Could not get event type: " + eventTypeName, e);
        }
    }

    public void updateTimeline(final Timeline timeline) {
        timelineDbRepository.updateTimelime(timeline);
    }

}
