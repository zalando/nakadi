package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedTimelineException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TimelinesNotSupportedException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.NakadiTopicConfig;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.service.StaticStorageWorkerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
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
    private final AdminService adminService;
    private final FeatureToggleService featureToggleService;
    private final String compactedStorageName;
    // one man said, it is fine to add 11th argument
    private final SchemaProviderService schemaService;
    private final LocalSchemaRegistry localSchemaRegistry;
    private final NakadiRecordMapper nakadiRecordMapper;

    @Autowired
    public TimelineService(final EventTypeCache eventTypeCache,
                           final StorageDbRepository storageDbRepository,
                           final TimelineSync timelineSync,
                           final NakadiSettings nakadiSettings,
                           final TimelineDbRepository timelineDbRepository,
                           final TopicRepositoryHolder topicRepositoryHolder,
                           final TransactionTemplate transactionTemplate,
                           final AdminService adminService,
                           final FeatureToggleService featureToggleService,
                           @Value("${nakadi.timelines.storage.compacted}") final String compactedStorageName,
                           final SchemaProviderService schemaService,
                           final LocalSchemaRegistry localSchemaRegistry,
                           final NakadiRecordMapper nakadiRecordMapper) {
        this.eventTypeCache = eventTypeCache;
        this.storageDbRepository = storageDbRepository;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.timelineDbRepository = timelineDbRepository;
        this.topicRepositoryHolder = topicRepositoryHolder;
        this.transactionTemplate = transactionTemplate;
        this.adminService = adminService;
        this.featureToggleService = featureToggleService;
        this.compactedStorageName = compactedStorageName;
        this.schemaService = schemaService;
        this.localSchemaRegistry = localSchemaRegistry;
        this.nakadiRecordMapper = nakadiRecordMapper;
    }

    public Timeline createTimeline(final String eventTypeName, final String storageId)
            throws AccessDeniedException, TimelineException, TopicRepositoryException, InconsistentStateException,
            RepositoryProblemException, DbWriteOperationsBlockedException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create timeline: write operations on DB " +
                    "are blocked by feature flag.");
        }
        try {
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
                throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, eventType.asResource());
            }
            if (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                    eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE) {
                throw new TimelinesNotSupportedException("It is not possible to create a timeline " +
                        "for event type with 'compact' cleanup_policy");
            }

            final Storage storage = storageDbRepository.getStorage(storageId)
                    .orElseThrow(() -> new UnableProcessException("No storage with id: " + storageId));
            final Timeline activeTimeline = getActiveTimeline(eventType);
            final TopicRepository currentTopicRepo =
                    topicRepositoryHolder.getTopicRepository(activeTimeline.getStorage());
            final TopicRepository nextTopicRepo = topicRepositoryHolder.getTopicRepository(storage);
            final List<PartitionStatistics> partitionStatistics =
                    currentTopicRepo.loadTopicStatistics(Collections.singleton(activeTimeline));

            final NakadiTopicConfig nakadiTopicConfig = new NakadiTopicConfig(partitionStatistics.size(),
                    eventType.getCleanupPolicy(), Optional.ofNullable(eventType.getOptions().getRetentionTime()));
            final String newTopic = nextTopicRepo.createTopic(nakadiTopicConfig);
            final Timeline nextTimeline = Timeline.createTimeline(activeTimeline.getEventType(),
                    activeTimeline.getOrder() + 1, storage, newTopic, new Date());

            switchTimelines(activeTimeline, nextTimeline);

            return nextTimeline;
        } catch (final TopicCreationException | TopicConfigException | ServiceTemporarilyUnavailableException |
                InternalNakadiException e) {
            throw new TimelineException("Internal service error", e);
        } catch (final NoSuchEventTypeException e) {
            throw new NotFoundException("EventType \"" + eventTypeName + "\" does not exist");
        }
    }

    public void updateTimeLineForRepartition(final EventType eventType, final int partitions)
            throws NakadiBaseException {
        for (final Timeline timeline : getActiveTimelinesOrdered(eventType.getName())) {
            getTopicRepository(timeline.getStorage()).repartition(timeline.getTopic(), partitions);
        }

        for (final Timeline timeline : getActiveTimelinesOrdered(eventType.getName())) {
            final Timeline.KafkaStoragePosition latestPosition = StaticStorageWorkerFactory.get(timeline)
                    .getLatestPosition(timeline);
            if (latestPosition == null) {
                continue;
            }
            while (partitions - latestPosition.getOffsets().size() > 0) {
                latestPosition.getOffsets().add(Long.valueOf(-1));
            }
            timelineDbRepository.updateTimelime(timeline);
        }
    }

    public Timeline createDefaultTimeline(final EventTypeBase eventType, final int partitionsCount)
            throws TopicCreationException,
            InconsistentStateException,
            RepositoryProblemException,
            DuplicatedTimelineException,
            TimelineException,
            DbWriteOperationsBlockedException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create default timeline: write operations on DB " +
                    "are blocked by feature flag.");
        }

        Storage storage = storageDbRepository.getDefaultStorage()
                .orElseThrow(() -> new TopicCreationException("Default storage is not set"));

        final Optional<Long> retentionTime = Optional.ofNullable(eventType.getOptions().getRetentionTime());
        if (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE) {
            storage = storageDbRepository.getStorage(compactedStorageName).orElseThrow(() ->
                    new TopicCreationException("No storage defined for compacted topics"));
        }

        final NakadiTopicConfig nakadiTopicConfig = new NakadiTopicConfig(partitionsCount, eventType.getCleanupPolicy(),
                retentionTime);
        final TopicRepository repository = topicRepositoryHolder.getTopicRepository(storage);
        final String topic = repository.createTopic(nakadiTopicConfig);

        try {
            final Timeline timeline = Timeline.createTimeline(eventType.getName(), 1, storage, topic, new Date());
            timeline.setSwitchedAt(new Date());
            timelineDbRepository.createTimeline(timeline);
            return timeline;
        } catch (final InconsistentStateException | RepositoryProblemException | DuplicatedTimelineException e) {
            rollbackTopic(repository, topic);
            throw e;
        } catch (final Exception e) {
            rollbackTopic(repository, topic);
            throw new TimelineException("Failed to update event type cache, while creating timeline", e);
        }
    }

    public void rollbackTopic(final Timeline timeline) {
        final TopicRepository repo = topicRepositoryHolder.getTopicRepository(timeline.getStorage());
        repo.deleteTopic(timeline.getTopic());
    }

    private void rollbackTopic(final TopicRepository repository, final String topic) {
        try {
            repository.deleteTopic(topic);
        } catch (final TopicDeletionException ex) {
            LOG.error("Failed to delete topic while recovering from timeline creation failure", ex);
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

    public Timeline getActiveTimeline(final String eventTypeName) throws TimelineException {
        try {
            final Optional<Timeline> timeline = getActiveTimeline(eventTypeCache.getTimelinesOrdered(eventTypeName));
            return timeline.orElseThrow(() -> {
                        throw new TimelineException(String.format("No timelines for event type %s", eventTypeName));
                    });
        } catch (final InternalNakadiException e) {
            LOG.error("Failed to get timeline for event type {}", eventTypeName, e);
            throw new TimelineException("Failed to get timeline", e);
        }
    }

    public static Optional<Timeline> getActiveTimeline(final List<Timeline> timelines) {
        final ListIterator<Timeline> rIterator = timelines.listIterator(timelines.size());
        while (rIterator.hasPrevious()) {
            final Timeline toCheck = rIterator.previous();
            if (toCheck.getSwitchedAt() != null) {
                return Optional.of(toCheck);
            }
        }
        return Optional.empty();
    }

    public Timeline getActiveTimeline(final EventTypeBase eventType) throws TimelineException {
        return getActiveTimeline(eventType.getName());
    }

    public TopicRepository getTopicRepository(final Storage storage) {
        return topicRepositoryHolder.getTopicRepository(storage);
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
            throws InvalidCursorException {
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
            LOG.error(tx.getMessage(), tx);
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
            throws TimelineException,
            NotFoundException,
            InternalNakadiException,
            NoSuchEventTypeException {
        LOG.info("Deleting all timelines for event type {}", eventTypeName);
        final Multimap<TopicRepository, String> topicsToDelete = ArrayListMultimap.create();
        for (final Timeline timeline : getAllTimelinesOrdered(eventTypeName)) {
            if (!timeline.isDeleted()) {
                topicsToDelete.put(getTopicRepository(timeline), timeline.getTopic());
            }
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
            throw new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                    new ResourceImpl<EventType>(eventTypeName, ResourceImpl.EVENT_TYPE_RESOURCE, null, null));
        }

        try {
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            return timelineDbRepository.listTimelinesOrdered(eventType.getName());
        } catch (final NoSuchEventTypeException e) {
            throw new NotFoundException("EventType \"" + eventTypeName + "\" does not exist");
        } catch (final InternalNakadiException e) {
            throw new TimelineException("Could not get event type: " + eventTypeName, e);
        }
    }

    public void updateTimeline(final Timeline timeline) {
        timelineDbRepository.updateTimelime(timeline);
    }

}
