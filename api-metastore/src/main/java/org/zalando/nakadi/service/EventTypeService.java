package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.everit.json.schema.Schema;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.CompatibilityMode;
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
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.partitioning.PartitionResolver;
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
import org.zalando.nakadi.util.JsonUtils;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.SchemaIncompatibility;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS;
import static org.zalando.nakadi.domain.Feature.FORCE_EVENT_TYPE_AUTHZ;

@Component
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
    private final String etLogEventType;
    private final NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private final EventTypeOptionsValidator eventTypeOptionsValidator;
    private final AdminService adminService;
    private final RepartitioningService repartitioningService;
    private final JsonSchemaEnrichment jsonSchemaEnrichment;
    private final EventTypeCache eventTypeCache;

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
            @Value("${nakadi.kpi.event-types.nakadiEventTypeLog}") final String etLogEventType,
            final NakadiAuditLogPublisher nakadiAuditLogPublisher,
            final EventTypeOptionsValidator eventTypeOptionsValidator,
            final AdminService adminService,
            final RepartitioningService repartitioningService,
            final EventTypeCache eventTypeCache,
            final JsonSchemaEnrichment jsonSchemaEnrichment) {
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
        this.etLogEventType = etLogEventType;
        this.nakadiAuditLogPublisher = nakadiAuditLogPublisher;
        this.eventTypeOptionsValidator = eventTypeOptionsValidator;
        this.adminService = adminService;
        this.repartitioningService = repartitioningService;
        this.eventTypeCache = eventTypeCache;
        this.jsonSchemaEnrichment = jsonSchemaEnrichment;
    }

    public List<EventType> list() {
        return eventTypeRepository.list();
    }

    public void create(final EventTypeBase eventType, final boolean checkAuth)
            throws AuthorizationSectionException,
            TopicCreationException,
            InternalNakadiException,
            NoSuchPartitionStrategyException,
            DuplicatedEventTypeNameException,
            InvalidEventTypeException,
            DbWriteOperationsBlockedException,
            EventTypeOptionsValidationException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create event type: write operations on DB " +
                    "are blocked by feature flag.");
        }
        if (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT
                && featureToggleService.isFeatureEnabled(Feature.DISABLE_LOG_COMPACTION)) {
            throw new FeatureNotAvailableException("log compaction is not available",
                    Feature.DISABLE_LOG_COMPACTION);
        }
        eventTypeOptionsValidator.checkRetentionTime(eventType.getOptions());
        setDefaultEventTypeOptions(eventType);
        validateSchema(eventType);
        validateCompaction(eventType);
        enrichment.validate(eventType);
        partitionResolver.validate(eventType);
        if (featureToggleService.isFeatureEnabled(FORCE_EVENT_TYPE_AUTHZ) && eventType.getAuthorization() == null) {
            throw new AuthorizationSectionException("Authorization section is mandatory");
        }
        if (checkAuth) {
            authorizationValidator.validateAuthorization(eventType.asBaseResource());
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
        nakadiKpiPublisher.publish(etLogEventType, () -> new JSONObject()
                .put("event_type", eventType.getName())
                .put("status", "created")
                .put("category", eventType.getCategory())
                .put("authz", identifyAuthzState(eventType))
                .put("compatibility_mode", eventType.getCompatibilityMode()));

        nakadiAuditLogPublisher.publish(Optional.empty(), Optional.of(eventType),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.CREATED,
                eventType.getName());
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
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT) {
            throw new InvalidEventTypeException(
                    "cleanup_policy 'compact' is not available for 'undefined' event type category");
        }
    }

    private void validateCompactionUpdate(final EventType original, final EventTypeBase updatedET) {
        validateCompaction(updatedET);
        if (original.getCleanupPolicy() != updatedET.getCleanupPolicy()) {
            throw new InvalidEventTypeException("cleanup_policy can not be changed");
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

            if (featureToggleService.isFeatureEnabled(DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS) ||
                    !hasNonDeletableSubscriptions(eventType.getName())) {
                topicsToDelete = deleteEventTypeWithSubscriptions(eventTypeName);
            } else {
                throw new ConflictException("Can't remove event type " + eventTypeName + ", as it has subscriptions");
            }
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
        nakadiKpiPublisher.publish(etLogEventType, () -> new JSONObject()
                .put("event_type", eventTypeName)
                .put("status", "deleted")
                .put("category", eventType.getCategory())
                .put("authz", identifyAuthzState(eventType))
                .put("compatibility_mode", eventType.getCompatibilityMode()));

        nakadiAuditLogPublisher.publish(Optional.of(eventType), Optional.empty(),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.DELETED,
                eventType.getName());
    }

    private Multimap<TopicRepository, String> deleteEventTypeWithSubscriptions(final String eventType) {
        try {
            return transactionTemplate.execute(action -> {
                final List<Subscription> subscriptions = subscriptionRepository.listSubscriptions(
                        ImmutableSet.of(eventType), Optional.empty(), 0, 100000);
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


    private boolean hasNonDeletableSubscriptions(final String eventTypeName) {
        int offset = 0;
        List<Subscription> subs = subscriptionRepository.listSubscriptions(
                ImmutableSet.of(eventTypeName), Optional.empty(), offset, 20);
        while (!subs.isEmpty()) {
            for (final Subscription sub : subs) {
                if (!sub.getConsumerGroup().equals(nakadiSettings.getDeletableSubscriptionConsumerGroup())
                        || !sub.getOwningApplication()
                        .equals(nakadiSettings.getDeletableSubscriptionOwningApplication())) {
                    return true;
                }
            }
            offset += 20;
            subs = subscriptionRepository.listSubscriptions(
                    ImmutableSet.of(eventTypeName), Optional.empty(), offset, 20);
        }
        return false;
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
            EventTypeOptionsValidationException {
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
            authorizationValidator.validateAuthorization(original.asResource(), eventTypeBase.asBaseResource());
            validateName(eventTypeName, eventTypeBase);
            validateCompactionUpdate(original, eventTypeBase);
            validateSchema(eventTypeBase);
            validateAudience(original, eventTypeBase);
            partitionResolver.validate(eventTypeBase);
            eventType = schemaEvolutionService.evolve(original, eventTypeBase);
            repartitioningService.checkAndRepartition(original, eventType);
            updateRetentionTime(original, eventType);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
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
        nakadiKpiPublisher.publish(etLogEventType, () -> new JSONObject()
                .put("event_type", eventTypeName)
                .put("status", "updated")
                .put("category", eventTypeBase.getCategory())
                .put("authz", identifyAuthzState(eventTypeBase))
                .put("compatibility_mode", eventTypeBase.getCompatibilityMode()));

        nakadiAuditLogPublisher.publish(Optional.of(original), Optional.of(eventType),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.UPDATED,
                eventType.getName());
    }

    private void updateRetentionTime(final EventType original, final EventType eventType) {
        final Long newRetentionTime = eventType.getOptions().getRetentionTime();
        final Long oldRetentionTime = original.getOptions().getRetentionTime();
        if (oldRetentionTime == null) {
            // since we have some inconsistency in DB I will put here for a while
            throw new InconsistentStateException("Empty value for retention time in existing EventType");
        }
        boolean retentionTimeUpdated = false;
        try {
            if (newRetentionTime != null && !newRetentionTime.equals(oldRetentionTime)) {
                updateTopicRetentionTime(original.getName(), newRetentionTime);
            } else {
                eventType.setOptions(original.getOptions());
            }
            updateEventTypeInDB(eventType, newRetentionTime, oldRetentionTime);
            retentionTimeUpdated = true;
        } finally {
            if (!retentionTimeUpdated) {
                updateTopicRetentionTime(original.getName(), oldRetentionTime);
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

    private void updateTopicRetentionTime(final String eventTypeName, final Long retentionTime)
            throws InternalNakadiException, NoSuchEventTypeException {
        timelineService.getActiveTimelinesOrdered(eventTypeName)
                .forEach(timeline -> timelineService.getTopicRepository(timeline)
                        .setRetentionTime(timeline.getTopic(), retentionTime));
    }

    public EventType get(final String eventTypeName) throws NoSuchEventTypeException, InternalNakadiException {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
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

    private void validateSchema(final EventTypeBase eventType) throws InvalidEventTypeException {
        try {
            final String eventTypeSchema = eventType.getSchema().getSchema();

            JsonUtils.checkEventTypeSchemaValid(eventTypeSchema);

            final JSONObject schemaAsJson = new JSONObject(eventTypeSchema);

            if (schemaAsJson.has("type") && !Objects.equals("object", schemaAsJson.getString("type"))) {
                throw new InvalidEventTypeException("\"type\" of root element in schema can only be \"object\"");
            }

            final Schema schema = SchemaLoader.load(schemaAsJson);
            if (eventType.getCategory() == EventCategory.BUSINESS && schema.definesProperty("#/metadata")) {
                throw new InvalidEventTypeException("\"metadata\" property is reserved");
            }

            final List<String> orderingInstanceIds = eventType.getOrderingInstanceIds();
            final List<String> orderingKeyFields = eventType.getOrderingKeyFields();
            if (!orderingInstanceIds.isEmpty() && orderingKeyFields.isEmpty()) {
                throw new InvalidEventTypeException(
                        "`ordering_instance_ids` field can not be defined without defining `ordering_key_fields`");
            }
            final JSONObject effectiveSchemaAsJson = jsonSchemaEnrichment.effectiveSchema(eventType);
            final Schema effectiveSchema = SchemaLoader.load(effectiveSchemaAsJson);
            validateFieldsInSchema("ordering_key_fields", orderingKeyFields, effectiveSchema);
            validateFieldsInSchema("ordering_instance_ids", orderingInstanceIds, effectiveSchema);

            if (eventType.getCompatibilityMode() == CompatibilityMode.COMPATIBLE) {
                validateJsonSchemaConstraints(schemaAsJson);
            }
        } catch (final JSONException e) {
            throw new InvalidEventTypeException("schema must be a valid json");
        } catch (final SchemaException e) {
            throw new InvalidEventTypeException("schema must be a valid json-schema");
        }
    }

    private void validateJsonSchemaConstraints(final JSONObject schema) throws InvalidEventTypeException {
        final List<SchemaIncompatibility> incompatibilities = schemaEvolutionService.collectIncompatibilities(schema);

        if (!incompatibilities.isEmpty()) {
            final String errorMessage = incompatibilities.stream().map(Object::toString)
                    .collect(Collectors.joining(", "));
            throw new InvalidEventTypeException("Invalid schema: " + errorMessage);
        }
    }

    private void validateFieldsInSchema(final String fieldName, final List<String> fields, final Schema schema) {
        final List<String> absentFields = fields.stream()
                .filter(field -> !schema.definesProperty(convertToJSONPointer(field)))
                .collect(Collectors.toList());
        if (!absentFields.isEmpty()) {
            throw new InvalidEventTypeException(fieldName + " " + absentFields + " absent in schema");
        }
    }


    private String convertToJSONPointer(final String value) {
        return value.replaceAll("\\.", "/");
    }
}
