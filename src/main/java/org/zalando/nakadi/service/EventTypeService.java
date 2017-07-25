package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.everit.json.schema.Schema;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.ConflictException;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FeatureToggleService;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_PARTITIONS_KEYS;
import org.zalando.nakadi.util.JsonUtils;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.SchemaIncompatibility;

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

    @Autowired
    public EventTypeService(final EventTypeRepository eventTypeRepository,
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
                            final NakadiSettings nakadiSettings) {
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
    }

    public List<EventType> list() {
        return eventTypeRepository.list();
    }

    public Result<Void> create(final EventTypeBase eventType) {
        final TopicRepository topicRepository = timelineService.getDefaultTopicRepository();
        try {
            validateSchema(eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
            authorizationValidator.validateAuthorization(eventType.getAuthorization());

            final String topicName = topicRepository.createTopic(
                    partitionsCalculator.getBestPartitionsCount(eventType.getDefaultStatistic()),
                    eventType.getOptions().getRetentionTime());
            eventType.setTopic(topicName);

            boolean eventTypeCreated = false;
            try {
                eventTypeRepository.saveEventType(eventType);
                eventTypeCreated = true;
            } finally {
                if (!eventTypeCreated) {
                    try {
                        topicRepository.deleteTopic(topicName);
                    } catch (final TopicDeletionException ex) {
                        LOG.error("failed to delete topic for event type that failed to be created", ex);
                    }
                }
            }
            return Result.ok();
        } catch (final InvalidEventTypeException | NoSuchPartitionStrategyException |
                DuplicatedEventTypeNameException e) {
            LOG.debug("Failed to create EventType.", e);
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Error creating event type " + eventType, e);
            return Result.problem(e.asProblem());
        }
    }

    public void delete(final String eventTypeName) throws EventTypeDeletionException, AccessDeniedException,
            NoEventTypeException, ConflictException, ServiceTemporarilyUnavailableException {
        Closeable deletionCloser = null;
        Multimap<TopicRepository, String> topicsToDelete = null;
        try {
            deletionCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final Optional<EventType> eventTypeOpt = eventTypeRepository.findByNameO(eventTypeName);
            if (!eventTypeOpt.isPresent()) {
                throw new NoEventTypeException("EventType \"" + eventTypeName + "\" does not exist.");
            }
            final EventType eventType = eventTypeOpt.get();

            authorizationValidator.authorizeEventTypeAdmin(eventType);

            final List<Subscription> subscriptions = subscriptionRepository.listSubscriptions(
                    ImmutableSet.of(eventTypeName), Optional.empty(), 0, 1);
            if (!subscriptions.isEmpty()) {
                throw new ConflictException("Can't remove event type " + eventTypeName
                        + ", as it has subscriptions");
            }
            topicsToDelete = transactionTemplate.execute(action -> {
                return deleteEventType(eventTypeName);
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } catch (final NakadiException e) {
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
    }

    public void update(final String eventTypeName,
                       final EventTypeBase eventTypeBase,
                       final Client client)
            throws TopicConfigException,
            InconsistentStateException,
            NakadiRuntimeException,
            ServiceTemporarilyUnavailableException,
            UnableProcessException {
        Closeable updatingCloser = null;
        try {
            updatingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final EventType original = eventTypeRepository.findByName(eventTypeName);

            authorizationValidator.authorizeEventTypeAdmin(original);
            authorizationValidator.validateAuthorization(original, eventTypeBase);
            validateName(eventTypeName, eventTypeBase);
            validateSchema(eventTypeBase);
            partitionResolver.validate(eventTypeBase);
            final EventType eventType = schemaEvolutionService.evolve(original, eventTypeBase);
            eventType.setDefaultStatistic(
                    validateStatisticsUpdate(original.getDefaultStatistic(), eventType.getDefaultStatistic()));
            updateRetentionTime(original, eventType);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new ServiceTemporarilyUnavailableException(
                    "Event type is currently in maintenance, please repeat request", e);
        } catch (final NakadiException e) {
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
    }

    private void updateRetentionTime(final EventType original, final EventType eventType) throws NakadiException {
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
                                     final Long oldRetentionTime) throws NakadiException {
        final NakadiException exception = transactionTemplate.execute(action -> {
            try {
                updateTimelinesCleanup(eventType.getName(), newRetentionTime, oldRetentionTime);
                eventTypeRepository.update(eventType);
                return null;
            } catch (final NakadiException e) {
                return e;
            }
        });
        if (exception != null) {
            throw exception;
        }
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

    public Result<EventType> get(final String eventTypeName) {
        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            return Result.ok(eventType);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", eventTypeName);
            return Result.problem(e.asProblem());
        } catch (final InternalNakadiException e) {
            LOG.error("Problem loading event type " + eventTypeName, e);
            return Result.problem(e.asProblem());
        }
    }

    private Multimap<TopicRepository, String> deleteEventType(final String eventTypeName)
            throws EventTypeUnavailableException, EventTypeDeletionException {
        try {
            final Multimap<TopicRepository, String> topicsToDelete =
                    timelineService.deleteAllTimelinesForEventType(eventTypeName);
            eventTypeRepository.removeEventType(eventTypeName);
            return topicsToDelete;
        } catch (TopicDeletionException e) {
            LOG.error("Problem deleting kafka topic for event type " + eventTypeName, e);
            throw new EventTypeUnavailableException("Failed to delete Kafka topic for event type " + eventTypeName);
        } catch (TimelineException | NotFoundException e) {
            LOG.error("Problem deleting timeline for event type " + eventTypeName, e);
            throw new EventTypeDeletionException("Failed to delete timelines for event type " + eventTypeName);
        } catch (NakadiException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            throw new EventTypeDeletionException("Failed to delete event type " + eventTypeName);
        }
    }

    private EventTypeStatistics validateStatisticsUpdate(
            final EventTypeStatistics existing,
            final EventTypeStatistics newStatistics) throws InvalidEventTypeException {
        if (existing != null && newStatistics == null) {
            return existing;
        }
        if (!Objects.equals(existing, newStatistics)) {
            throw new InvalidEventTypeException("default statistics must not be changed");
        }
        return newStatistics;
    }

    private void validateName(final String name, final EventTypeBase eventType) throws InvalidEventTypeException {
        if (!eventType.getName().equals(name)) {
            throw new InvalidEventTypeException("path does not match resource name");
        }
    }

    private void validateSchema(final EventTypeBase eventType) throws InvalidEventTypeException {
        try {
            final String eventTypeSchema = eventType.getSchema().getSchema();

            JsonUtils.checkEventTypeSchemaValid(eventTypeSchema);

            final JSONObject schemaAsJson = new JSONObject(eventTypeSchema);
            final Schema schema = SchemaLoader.load(schemaAsJson);

            if (eventType.getCategory() == EventCategory.BUSINESS && schema.definesProperty("#/metadata")) {
                throw new InvalidEventTypeException("\"metadata\" property is reserved");
            }

            if (featureToggleService.isFeatureEnabled(CHECK_PARTITIONS_KEYS)) {
                validatePartitionKeys(schema, eventType);
            }

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

    private void validatePartitionKeys(final Schema schema, final EventTypeBase eventType)
            throws InvalidEventTypeException, JSONException, SchemaException {
        final List<String> absentFields = eventType.getPartitionKeyFields().stream()
                .filter(field -> !schema.definesProperty(convertToJSONPointer(field)))
                .collect(Collectors.toList());
        if (!absentFields.isEmpty()) {
            throw new InvalidEventTypeException("partition_key_fields " + absentFields + " absent in schema");
        }
    }

    private String convertToJSONPointer(final String value) {
        return value.replaceAll("\\.", "/");
    }
}
