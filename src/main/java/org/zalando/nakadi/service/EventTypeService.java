package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import org.everit.json.schema.Schema;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_PARTITIONS_KEYS;

@Component
public class EventTypeService {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeService.class);

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final UUIDGenerator uuidGenerator;
    private final FeatureToggleService featureToggleService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SchemaEvolutionService schemaEvolutionService;

    @Autowired
    public EventTypeService(final EventTypeRepository eventTypeRepository, final TopicRepository topicRepository,
                            final PartitionResolver partitionResolver, final Enrichment enrichment,
                            final UUIDGenerator uuidGenerator,
                            final FeatureToggleService featureToggleService,
                            final SubscriptionDbRepository subscriptionRepository,
                            final SchemaEvolutionService schemaEvolutionService) {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.uuidGenerator = uuidGenerator;
        this.featureToggleService = featureToggleService;
        this.subscriptionRepository = subscriptionRepository;
        this.schemaEvolutionService = schemaEvolutionService;
    }

    public List<EventType> list() {
        return eventTypeRepository.list();
    }

    public Result<Void> create(final EventTypeBase eventType) {
        try {
            assignTopic(eventType);
            validateSchema(eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
            final EventType savedEventType =  eventTypeRepository.saveEventType(eventType);
            topicRepository.createTopic(savedEventType);
            return Result.ok();
        } catch (final InvalidEventTypeException | NoSuchPartitionStrategyException |
                DuplicatedEventTypeNameException e) {
            LOG.debug("Failed to create EventType.", e);
            return Result.problem(e.asProblem());
        } catch (final TopicCreationException e) {
            LOG.error("Problem creating kafka topic. Rolling back event type database registration.", e);

            try {
                eventTypeRepository.removeEventType(eventType.getTopic());
            } catch (final NakadiException e1) {
                return Result.problem(e.asProblem());
            }
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Error creating event type " + eventType, e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<Void> delete(final String eventTypeName, final Client client) {
        try {
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
        } catch (final TopicDeletionException e) {
            LOG.error("Problem deleting kafka topic " + eventTypeName, e);
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<Void> update(final String eventTypeName, final EventTypeBase eventTypeBase, final Client client) {
        try {
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
        } catch (final InvalidEventTypeException e) {
            return Result.problem(e.asProblem());
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", eventTypeName);
            return Result.problem(e.asProblem());
        } catch (final NakadiException e) {
            LOG.error("Unable to update event type", e);
            return Result.problem(e.asProblem());
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

    private EventTypeStatistics validateStatisticsUpdate(final EventTypeStatistics existing,
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
            final JSONObject schemaAsJson = new JSONObject(eventTypeSchema);
            final String parsedSchemaAsString = schemaAsJson.toString();

            // bugfix ARUHA-563
            // https://github.com/FasterXML/jackson-databind/issues/726
            if (!parsedSchemaAsString.equals(eventTypeSchema)) {
                throw new InvalidEventTypeException("schema must be a valid json");
            }

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

    private void assignTopic(final EventTypeBase eventType) {
        eventType.setTopic(uuidGenerator.randomUUID().toString());
    }

    private String convertToJSONPointer(final String value) {
        return value.replaceAll("\\.", "/");
    }
}
