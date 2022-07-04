package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.AuthorizationSectionException;
import org.zalando.nakadi.exceptions.runtime.CannotAddPartitionToTopicException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidOwningApplicationException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.SchemaEvolutionException;
import org.zalando.nakadi.exceptions.runtime.SchemaValidationException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.kpi.event.NakadiEventTypeLog;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.zalando.nakadi.domain.Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS;
import static org.zalando.nakadi.domain.Feature.FORCE_EVENT_TYPE_AUTHZ;

@Component
@DependsOn({"storageService"})
public class EventTypeService {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeService.class);

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SchemaEvolutionService schemaEvolutionService;
    private final PartitionsCalculator partitionsCalculator;
    private final FeatureToggleService featureToggleService;
    private final AuthorizationValidator authorizationValidator;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final TransactionTemplate transactionTemplate;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private final EventTypeOptionsValidator eventTypeOptionsValidator;
    private final AdminService adminService;
    private final ApplicationService applicationService;

    private final EventTypeCache eventTypeCache;
    private final SchemaService schemaService;

    @Autowired
    public EventTypeService(
            final EventTypeRepository eventTypeRepository,
            final TimelineService timelineService,
            final PartitionResolver partitionResolver,
            final Enrichment enrichment,
            final SubscriptionDbRepository subscriptionRepository,
            final SchemaEvolutionService schemaEvolutionService,
            final PartitionsCalculator partitionsCalculator,
            final FeatureToggleService featureToggleService,
            final AuthorizationValidator authorizationValidator,
            final TimelineSync timelineSync,
            final TransactionTemplate transactionTemplate,
            final NakadiSettings nakadiSettings,
            final NakadiKpiPublisher nakadiKpiPublisher,
            final NakadiAuditLogPublisher nakadiAuditLogPublisher,
            final EventTypeOptionsValidator eventTypeOptionsValidator,
            final EventTypeCache eventTypeCache,
            final SchemaService schemaService,
            final AdminService adminService,
            final ApplicationService applicationService) {
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.subscriptionRepository = subscriptionRepository;
        this.schemaEvolutionService = schemaEvolutionService;
        this.partitionsCalculator = partitionsCalculator;
        this.featureToggleService = featureToggleService;
        this.authorizationValidator = authorizationValidator;
        this.timelineSync = timelineSync;
        this.transactionTemplate = transactionTemplate;
        this.nakadiSettings = nakadiSettings;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.nakadiAuditLogPublisher = nakadiAuditLogPublisher;
        this.eventTypeOptionsValidator = eventTypeOptionsValidator;
        this.adminService = adminService;
        this.eventTypeCache = eventTypeCache;
        this.schemaService = schemaService;
        this.applicationService = applicationService;
    }

    public List<EventType> list(
            @Nullable final AuthorizationAttribute writer,
            @Nullable final String owningApplication) {
        return eventTypeRepository.list(Optional.ofNullable(writer), Optional.ofNullable(owningApplication));
    }

    public void create(final EventTypeBase eventType, final boolean checkAuth)
            throws AuthorizationSectionException,
            TopicCreationException,
            InternalNakadiException,
            NoSuchPartitionStrategyException,
            DuplicatedEventTypeNameException,
            InvalidEventTypeException,
            DbWriteOperationsBlockedException,
            EventTypeOptionsValidationException,
            InvalidOwningApplicationException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create event type: write operations on DB " +
                    "are blocked by feature flag.");
        }
        eventTypeOptionsValidator.checkRetentionTime(eventType.getOptions());
        setDefaultEventTypeOptions(eventType);
        try {
            schemaService.validateSchema(eventType);
        } catch (final SchemaValidationException e) {
            throw new InvalidEventTypeException(e);
        }
        validateCompaction(eventType);
        enrichment.validate(eventType);
        partitionResolver.validate(eventType);
        if (featureToggleService.isFeatureEnabled(FORCE_EVENT_TYPE_AUTHZ) && eventType.getAuthorization() == null) {
            throw new AuthorizationSectionException("Authorization section is mandatory");
        }
        if (checkAuth) {
            authorizationValidator.validateAuthorization(eventType.asBaseResource());
        }

        validateOwningApplication(null, eventType.getOwningApplication());

        validateEventOwnerSelector(eventType);

        if (eventType.getAnnotations() == null) {
            eventType.setAnnotations(new HashMap<>());
        }
        if (eventType.getLabels() == null) {
            eventType.setLabels(new HashMap<>());
        }

        final AtomicReference<EventType> createdEventType = new AtomicReference<>(null);
        final AtomicReference<Timeline> createdTimeline = new AtomicReference<>(null);
        try {
            transactionTemplate.execute(action -> {
                createdEventType.set(eventTypeRepository.saveEventType(eventType));
                createdTimeline.set(timelineService.createDefaultTimeline(eventType,
                        partitionsCalculator.getBestPartitionsCount(eventType.getDefaultStatistic())));
                return null;
            });
            eventTypeCache.invalidate(eventType.getName());
        } catch (final RuntimeException ex) {
            LOG.error("Failed to create event type, will clean up created data", ex);
            if (null != createdTimeline.get()) {
                try {
                    timelineService.rollbackTopic(createdTimeline.get());
                } catch (final RuntimeException ex2) {
                    LOG.warn("Failed to rollback timeline", ex2);
                }
            }
            if (null != createdEventType.get()) {
                try {
                    eventTypeRepository.removeEventType(createdEventType.get().getName());
                } catch (RuntimeException ex2) {
                    LOG.warn("Failed to rollback event type creation (which is highly expected)", ex2);
                }
            }
            if (ex instanceof NakadiBaseException) {
                throw ex;
            } else {
                throw new InternalNakadiException("Failed to create event type: " + ex.getMessage(), ex);
            }
        }
        nakadiKpiPublisher.publish(() -> NakadiEventTypeLog.newBuilder()
                .setEventType(eventType.getName())
                .setStatus("created")
                .setCategory(eventType.getCategory().name())
                .setAuthz(identifyAuthzState(eventType))
                .setCompatibilityMode(eventType.getCompatibilityMode().name())
                .build());

        nakadiAuditLogPublisher.publish(Optional.empty(), Optional.of(eventType),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.CREATED,
                eventType.getName());
    }

    private void validateOwningApplication(
            @Nullable final String oldOwningApplication, final String newOwningApplication)
            throws InvalidOwningApplicationException {
        if (featureToggleService.isFeatureEnabled(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)) {
            if (!Objects.equals(oldOwningApplication, newOwningApplication)) {
                if (!applicationService.exists(newOwningApplication)) {
                    throw new InvalidOwningApplicationException(newOwningApplication);
                }
            }
        }
    }

    public void createIfMissing(final EventTypeBase eventType, final boolean checkAuth)
            throws AuthorizationSectionException,
            TopicCreationException,
            InternalNakadiException,
            NoSuchPartitionStrategyException,
            DuplicatedEventTypeNameException,
            InvalidEventTypeException,
            DbWriteOperationsBlockedException,
            EventTypeOptionsValidationException {
        try {
            eventTypeCache.getEventType(eventType.getName());
            LOG.info("Event-type {} already exists", eventType.getName());
        } catch (final NoSuchEventTypeException noSuchEventTypeException) {
            LOG.info("Creating event-type {} as it is missing", eventType.getName());
            create(eventType, checkAuth);
        }
    }

    private void validateCompaction(final EventTypeBase eventType) throws
            InvalidEventTypeException {
        if (eventType.getCategory() == EventCategory.UNDEFINED &&
                (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                        eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE)) {
            throw new InvalidEventTypeException(
                    "cleanup_policy 'compact' and 'compact_and_delete' is not available " +
                            "for 'undefined' event type category");
        }
    }

    private void validateCompactionUpdate(final EventType original, final EventTypeBase updatedET) {
        validateCompaction(updatedET);
        if (updatedET.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE
                && original.getCleanupPolicy() == CleanupPolicy.DELETE) {
            return;
        }
        if (original.getCleanupPolicy() != updatedET.getCleanupPolicy()) {
            throw new InvalidEventTypeException(
                    "Invalid cleanup_policy change: " + original.getCleanupPolicy().name() +
                            " cannot be changed to " + updatedET.getCleanupPolicy().name()
            );
        }
    }

    private String identifyAuthzState(final EventTypeBase eventType) {
        if (eventType.getAuthorization() == null) {
            return "disabled";
        }
        return "enabled";
    }

    private void setDefaultEventTypeOptions(final EventTypeBase eventType) {
        final EventTypeOptions options = eventType.getOptions();
        if (options == null) {
            final EventTypeOptions eventTypeOptions = new EventTypeOptions();
            eventTypeOptions.setRetentionTime(nakadiSettings.getDefaultTopicRetentionMs());
            eventType.setOptions(eventTypeOptions);
        } else if (options.getRetentionTime() == null) {
            options.setRetentionTime(nakadiSettings.getDefaultTopicRetentionMs());
        }
    }

    public void delete(final String eventTypeName) throws EventTypeDeletionException,
            AccessDeniedException, NoSuchEventTypeException, ConflictException, ServiceTemporarilyUnavailableException,
            DbWriteOperationsBlockedException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot delete event type: write operations on DB " +
                    "are blocked by feature flag.");
        }
        Closeable deletionCloser = null;
        final EventType eventType;
        final Multimap<TopicRepository, String> topicsToDelete;
        try {
            deletionCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final Optional<EventType> eventTypeOpt = eventTypeCache.getEventTypeIfExists(eventTypeName);
            if (!eventTypeOpt.isPresent()) {
                throw new NoSuchEventTypeException("EventType \"" + eventTypeName + "\" does not exist.");
            }
            eventType = eventTypeOpt.get();

            authorizationValidator.authorizeEventTypeView(eventType);
            authorizationValidator.authorizeEventTypeAdmin(eventType);

            if (eventType.getAuthorization() == null && featureToggleService.isFeatureEnabled(FORCE_EVENT_TYPE_AUTHZ)) {
                throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, eventType.asResource(),
                        "You cannot delete event-type without authorization.");
            }

            topicsToDelete = deleteEventTypeWithSubscriptions(eventTypeName);
            eventTypeCache.invalidate(eventTypeName);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } catch (final InternalNakadiException | ServiceTemporarilyUnavailableException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            throw new EventTypeDeletionException("Failed to delete event type " + eventTypeName);
        } finally {
            try {
                if (deletionCloser != null) {
                    deletionCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
        if (topicsToDelete != null) {
            for (final TopicRepository topicRepository : topicsToDelete.keySet()) {
                for (final String topic : topicsToDelete.get(topicRepository)) {
                    try {
                        topicRepository.deleteTopic(topic);
                    } catch (TopicDeletionException e) {
                        // If a timeline was marked as deleted, then the topic does not exist, and we should proceed.
                        LOG.info("Could not delete topic " + topic, e);
                    }
                }
            }
        }
        nakadiKpiPublisher.publish(() -> NakadiEventTypeLog.newBuilder()
                .setEventType(eventTypeName)
                .setStatus("deleted")
                .setCategory(eventType.getCategory().name())
                .setAuthz(identifyAuthzState(eventType))
                .setCompatibilityMode(eventType.getCompatibilityMode().name())
                .build());

        nakadiAuditLogPublisher.publish(Optional.of(eventType), Optional.empty(),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.DELETED,
                eventType.getName());
    }

    private Multimap<TopicRepository, String> deleteEventTypeWithSubscriptions(final String eventType) {
        try {
            return transactionTemplate.execute(action -> {
                final Set<String> eventTypes = ImmutableSet.of(eventType);
                eventTypeRepository.listEventTypesWithRowLock(eventTypes, EventTypeRepository.RowLockMode.UPDATE);

                final List<Subscription> subscriptions = subscriptionRepository.listAllSubscriptionsFor(eventTypes);

                if (!(featureToggleService.isFeatureEnabled(DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS)
                        || onlyDeletableSubscriptions(subscriptions))) {
                    throw new ConflictException("Can't remove event type " + eventType +
                            ", as it has subscriptions");
                }

                subscriptions.forEach(s -> {
                    try {
                        subscriptionRepository.deleteSubscription(s.getId());
                    } catch (final NoSuchSubscriptionException e) {
                        // should not happen as we are inside transaction
                        throw new InconsistentStateException("Subscription to be deleted is not found", e);
                    }
                });
                return deleteEventType(eventType);
            });
        } catch (final TransactionException e) {
            throw new InconsistentStateException("Failed to delete event-type because of race condition in DB", e);
        }
    }

    private boolean onlyDeletableSubscriptions(final List<Subscription> subscriptions) {
        final String owningApplication = nakadiSettings.getDeletableSubscriptionOwningApplication();
        final String consumerGroup = nakadiSettings.getDeletableSubscriptionConsumerGroup();
        return subscriptions.stream().allMatch(sub ->
                sub.getOwningApplication().equals(owningApplication) && sub.getConsumerGroup().equals(consumerGroup));
    }

    public void update(final String eventTypeName,
                       final EventTypeBase eventTypeBase)
            throws TopicConfigException,
            InconsistentStateException,
            NakadiRuntimeException,
            ServiceTemporarilyUnavailableException,
            UnableProcessException,
            DbWriteOperationsBlockedException,
            CannotAddPartitionToTopicException,
            EventTypeOptionsValidationException,
            InvalidOwningApplicationException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot update event type: write operations on DB " +
                    "are blocked by feature flag.");
        }
        Closeable updatingCloser = null;
        final EventType original;
        final EventType eventType;
        try {
            updatingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());
            original = eventTypeRepository.findByName(eventTypeName);

            if (featureToggleService.isFeatureEnabled(FORCE_EVENT_TYPE_AUTHZ)
                    && eventTypeBase.getAuthorization() == null) {
                throw new AuthorizationSectionException("Authorization section is mandatory");
            }

            authorizationValidator.authorizeEventTypeView(original);
            if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
                eventTypeOptionsValidator.checkRetentionTime(eventTypeBase.getOptions());
                authorizationValidator.authorizeEventTypeAdmin(original);
                validateEventOwnerSelectorUnchanged(original, eventTypeBase);
            }
            validateEventOwnerSelector(eventTypeBase);

            authorizationValidator.validateAuthorization(original.asResource(), eventTypeBase.asBaseResource());
            validateName(eventTypeName, eventTypeBase);
            validateCompactionUpdate(original, eventTypeBase);
            schemaService.validateSchema(eventTypeBase);
            validateAudience(original, eventTypeBase);
            partitionResolver.validate(eventTypeBase);
            validateOwningApplication(original.getOwningApplication(), eventTypeBase.getOwningApplication());
            eventType = schemaEvolutionService.evolve(original, eventTypeBase);
            validateStatisticsUpdate(original, eventType);
            updateAnnotationsAndLabels(original, eventType);
            updateRetentionTime(original, eventType);
            updateEventType(original, eventType);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
        } catch (final SchemaValidationException e) {
            LOG.warn("Schema validation failed {}", e.getMessage());
            throw new InvalidEventTypeException(e);
        } catch (final SchemaEvolutionException e) {
            LOG.warn("Schema evolution failed {}", e.getMessage());
            throw new InvalidEventTypeException(e);
        } catch (final InternalNakadiException e) {
            LOG.error("Unable to update event type", e);
            throw new NakadiRuntimeException(e);
        } finally {
            try {
                if (updatingCloser != null) {
                    updatingCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
        nakadiKpiPublisher.publish(() -> NakadiEventTypeLog.newBuilder()
                .setEventType(eventTypeName)
                .setStatus("updated")
                .setCategory(eventTypeBase.getCategory().name())
                .setAuthz(identifyAuthzState(eventTypeBase))
                .setCompatibilityMode(eventTypeBase.getCompatibilityMode().name())
                .build());

        nakadiAuditLogPublisher.publish(Optional.of(original), Optional.of(eventType),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.UPDATED,
                eventType.getName());
    }

    private void updateRetentionTime(final EventType original, final EventType eventType) {
        if (eventType.getOptions() == null || eventType.getOptions().getRetentionTime() == null) {
            eventType.setOptions(original.getOptions());
        }
        setDefaultEventTypeOptions(original); // fixes a problem where the event type has no explicit retention time
        setDefaultEventTypeOptions(eventType);
    }

    private void updateAnnotationsAndLabels(final EventType original, final EventType eventType)
            throws InvalidEventTypeException {

        if (eventType.getAnnotations() == null) {
            eventType.setAnnotations(original.getAnnotations());
        }
        if (eventType.getLabels() == null) {
            eventType.setLabels(original.getLabels());
        }
    }

    private void updateEventType(final EventType original, final EventType eventType) {
        final Long newRetentionTime = eventType.getOptions().getRetentionTime();
        final Long oldRetentionTime = original.getOptions().getRetentionTime();
        boolean updatedDefinitions = false;
        try {
            updateTopicConfig(original.getName(), newRetentionTime, eventType.getCleanupPolicy());
            updateEventTypeInDB(eventType, newRetentionTime, oldRetentionTime);
            updatedDefinitions = true;
        } finally {
            if (!updatedDefinitions) {
                LOG.warn("Error while updating topic configuration for {}, attempting to revert.", original.getName());
                updateTopicConfig(original.getName(), oldRetentionTime, original.getCleanupPolicy());
            }
        }
    }

    private void updateEventTypeInDB(final EventType eventType, final Long newRetentionTime,
                                     final Long oldRetentionTime)
            throws InternalNakadiException {
        transactionTemplate.execute(action -> {
            updateTimelinesCleanup(eventType.getName(), newRetentionTime, oldRetentionTime);
            eventTypeRepository.update(eventType);
            return null;
        });
        eventTypeCache.invalidate(eventType.getName());
    }

    private void updateTimelinesCleanup(final String eventType, final Long newRetentionTime,
                                        final Long oldRetentionTime)
            throws InternalNakadiException, NoSuchEventTypeException {

        if (newRetentionTime != null && !newRetentionTime.equals(oldRetentionTime)) {
            final long retentionDiffMs = newRetentionTime - oldRetentionTime;
            final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventType);

            for (final Timeline timeline : timelines) {
                if (timeline.getCleanedUpAt() != null) {
                    timeline.setCleanedUpAt(new Date(timeline.getCleanedUpAt().getTime() + retentionDiffMs));
                    timelineService.updateTimeline(timeline);
                }
            }
        }
    }

    private void updateTopicConfig(final String eventTypeName, final Long retentionTime,
                                   final CleanupPolicy cleanupPolicy)
            throws InternalNakadiException, NoSuchEventTypeException {
        timelineService.getActiveTimelinesOrdered(eventTypeName)
                .forEach(timeline -> timelineService.getTopicRepository(timeline)
                        .updateTopicConfig(timeline.getTopic(), retentionTime, cleanupPolicy));
    }

    public EventType get(final String eventTypeName) throws NoSuchEventTypeException, InternalNakadiException {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        return eventType;
    }

    /**
     * Same as get(final String eventTypeName), but without using cache.
     *
     * @param eventTypeName Name of event-type to be fetched
     * @return EventType
     * @throws NoSuchEventTypeException if event-type does not exist
     */
    public EventType fetchFromRepository(final String eventTypeName) throws NoSuchEventTypeException {
        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        return eventType;
    }

    private Multimap<TopicRepository, String> deleteEventType(final String eventTypeName)
            throws EventTypeDeletionException {
        try {
            final Multimap<TopicRepository, String> topicsToDelete =
                    timelineService.deleteAllTimelinesForEventType(eventTypeName);
            eventTypeRepository.removeEventType(eventTypeName);
            return topicsToDelete;
        } catch (TimelineException | NotFoundException e) {
            LOG.error("Problem deleting timeline for event type " + eventTypeName, e);
            throw new EventTypeDeletionException("Failed to delete timelines for event type " + eventTypeName);
        } catch (InternalNakadiException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            throw new EventTypeDeletionException("Failed to delete event type " + eventTypeName);
        }
    }

    private void validateStatisticsUpdate(
            final EventType originalEventType,
            final EventTypeBase newEventType) throws InvalidEventTypeException {
        if (newEventType.getDefaultStatistic() == null) {
            newEventType.setDefaultStatistic(originalEventType.getDefaultStatistic());
        } else if (!Objects.equals(originalEventType.getDefaultStatistic(), newEventType.getDefaultStatistic())) {
            throw new InvalidEventTypeException("default statistics must not be changed");
        }
    }

    private void validateName(final String name, final EventTypeBase eventType) throws InvalidEventTypeException {
        if (!eventType.getName().equals(name)) {
            throw new InvalidEventTypeException("path does not match resource name");
        }
    }

    private void validateAudience(final EventType original, final EventTypeBase eventTypeBase) throws
            InvalidEventTypeException {
        if (original.getAudience() != null && eventTypeBase.getAudience() == null) {
            throw new InvalidEventTypeException("event audience must not be set back to null");
        }
    }

    static void validateEventOwnerSelector(final EventTypeBase eventType) {
        final EventOwnerSelector selector = eventType.getEventOwnerSelector();
        if (selector != null) {
            if (selector.getType() == EventOwnerSelector.Type.METADATA && selector.getValue() != null) {
                throw new InvalidEventTypeException(
                        "event_owner_selector specifying value for type 'metadata' is not supported");
            }
        }
    }

    private void validateEventOwnerSelectorUnchanged(final EventType original, final EventTypeBase eventTypeBase) throws
            InvalidEventTypeException {
        final EventOwnerSelector originalEventOwnerSelector = original.getEventOwnerSelector();
        final EventOwnerSelector updatedEventOwnerSelector = eventTypeBase.getEventOwnerSelector();
        if (updatedEventOwnerSelector != null && originalEventOwnerSelector != null) {
            if (!updatedEventOwnerSelector.equals(originalEventOwnerSelector)) {
                throw new InvalidEventTypeException(
                        String.format("event_owner_selector must not be changed, original: %s, updated: %s",
                                originalEventOwnerSelector.toString(), updatedEventOwnerSelector.toString()));
            }
        } else if (updatedEventOwnerSelector == null && originalEventOwnerSelector != null) {
            throw new InvalidEventTypeException("event_owner_selector can't be set back to null");
        }
    }
}
