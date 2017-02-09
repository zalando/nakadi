package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.SchemaIncompatibility;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_PARTITIONS_KEYS;

@Component
public class EventTypeService {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeService.class);

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SchemaEvolutionService schemaEvolutionService;
    private final PartitionsCalculator partitionsCalculator;
    private final FeatureToggleService featureToggleService;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public EventTypeService(final EventTypeRepository eventTypeRepository,
                            final TopicRepository topicRepository,
                            final PartitionResolver partitionResolver,
                            final Enrichment enrichment,
                            final SubscriptionDbRepository subscriptionRepository,
                            final SchemaEvolutionService schemaEvolutionService,
                            final PartitionsCalculator partitionsCalculator,
                            final FeatureToggleService featureToggleService,
                            final TimelineSync timelineSync,
                            final NakadiSettings nakadiSettings) {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.subscriptionRepository = subscriptionRepository;
        this.schemaEvolutionService = schemaEvolutionService;
        this.partitionsCalculator = partitionsCalculator;
        this.featureToggleService = featureToggleService;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
    }

    public List<EventType> list() {
        return eventTypeRepository.list();
    }

    public Result<Void> create(final EventTypeBase eventType) {
        try {
            validateSchema(eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
            final String topicName = topicRepository.createTopic(
                    partitionsCalculator.getBestPartitionsCount(eventType.getDefaultStatistic()),
                    eventType.getOptions().getRetentionTime());
            eventType.setTopic(topicName);
            eventTypeRepository.saveEventType(eventType);
            return Result.ok();
        } catch (final InvalidEventTypeException | NoSuchPartitionStrategyException |
                DuplicatedEventTypeNameException e) {
            LOG.debug("Failed to create EventType.", e);
            if (null != eventType.getTopic()) {
                try {
                    topicRepository.deleteTopic(eventType.getTopic());
                } catch (final TopicDeletionException ex) {
                    LOG.warn("failed to delete topic for event type that failed to be created", ex);
                }
            }
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Error creating event type " + eventType, e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<Void> delete(final String eventTypeName, final Client client) {
        Closeable deletionCloser = null;
        try {
            deletionCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final Optional<EventType> eventType = eventTypeRepository.findByNameO(eventTypeName);
            if (!eventType.isPresent()) {
                return Result.notFound("EventType \"" + eventTypeName + "\" does not exist.");
            }
            if (!client.idMatches(eventType.get().getOwningApplication())) {
                return Result.forbidden("You don't have access to this event type");
            }
            final List<Subscription> subscriptions = subscriptionRepository.listSubscriptions(
                    ImmutableSet.of(eventTypeName), Optional.empty(), 0, 1);
            if (!subscriptions.isEmpty()) {
                return Result.conflict("Not possible to remove event-type as it has subscriptions");
            }

            eventTypeRepository.removeEventType(eventTypeName);
            topicRepository.deleteTopic(eventType.get().getTopic());
            return Result.ok();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            return Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
                    "Event type is currently in maintenance, please repeat request"));
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            return Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
                    "Event type is currently in maintenance, please repeat request"));
        } catch (final TopicDeletionException e) {
            LOG.error("Problem deleting kafka topic " + eventTypeName, e);
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            return Result.problem(e.asProblem());
        } finally {
            try {
                if (deletionCloser != null) {
                    deletionCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
    }

    public Result<Void> update(final String eventTypeName, final EventTypeBase eventTypeBase, final Client client) {
        Closeable updatingCloser = null;
        try {
            updatingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final EventType original = eventTypeRepository.findByName(eventTypeName);
            if (!client.idMatches(original.getOwningApplication())) {
                return Result.forbidden("You don't have access to this event type");
            }

            validateName(eventTypeName, eventTypeBase);
            validateSchema(eventTypeBase);
            final EventType eventType = schemaEvolutionService.evolve(original, eventTypeBase);
            eventType.setDefaultStatistic(
                    validateStatisticsUpdate(original.getDefaultStatistic(), eventType.getDefaultStatistic()));
            eventTypeRepository.update(eventType);
            return Result.ok();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            return Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
                    "Event type is currently in maintenance, please repeat request"));
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            return Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
                    "Event type is currently in maintenance, please repeat request"));
        } catch (final InvalidEventTypeException e) {
            return Result.problem(e.asProblem());
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", eventTypeName);
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Unable to update event type", e);
            return Result.problem(e.asProblem());
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
            final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

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
