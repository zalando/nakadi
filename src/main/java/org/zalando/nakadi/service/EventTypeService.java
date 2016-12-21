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
import org.springframework.transaction.annotation.Transactional;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.*;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.StorageWorkerFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;

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
    private final FeatureToggleService featureToggleService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final StorageWorkerFactory storageWorkerFactory;
    private final TimelineService timelineService;

    @Autowired
    public EventTypeService(final EventTypeRepository eventTypeRepository, final TopicRepository topicRepository,
                            final PartitionResolver partitionResolver, final Enrichment enrichment,
                            final FeatureToggleService featureToggleService,
                            final SubscriptionDbRepository subscriptionRepository,
                            final StorageWorkerFactory storageWorkerFactory,
                            final TimelineService timelineService) {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.featureToggleService = featureToggleService;
        this.subscriptionRepository = subscriptionRepository;
        this.storageWorkerFactory = storageWorkerFactory;
        this.timelineService = timelineService;
    }

    public List<EventType> list() {
        return eventTypeRepository.list();
    }

    @Transactional
    public Result<Void> create(final EventType eventType) {
        try {
            validateSchema(eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
            eventTypeRepository.saveEventType(eventType);
            timelineService.createTimelineForNewEventType(eventType);
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

    public Result<Void> update(final String eventTypeName, final EventType eventType, final Client client) {
        try {
            final EventType original = eventTypeRepository.findByName(eventTypeName);
            if (!client.idMatches(original.getOwningApplication())) {
                return Result.forbidden("You don't have access to this event type");
            }

            validateUpdate(eventTypeName, eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
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

    private void validateUpdate(final String name, final EventType eventType) throws NoSuchEventTypeException,
            InternalNakadiException, InvalidEventTypeException, NoSuchPartitionStrategyException {
        final EventType existingEventType = eventTypeRepository.findByName(name);

        validateName(name, eventType);
        validatePartitionKeys(Optional.empty(), eventType);
        validateSchemaChange(eventType, existingEventType);
        eventType.setDefaultStatistic(
                validateStatisticsUpdate(existingEventType.getDefaultStatistic(), eventType.getDefaultStatistic()));
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

    private void validateName(final String name, final EventType eventType) throws InvalidEventTypeException {
        if (!eventType.getName().equals(name)) {
            throw new InvalidEventTypeException("path does not match resource name");
        }
    }

    private void validateSchemaChange(final EventType eventType, final EventType existingEventType)
            throws InvalidEventTypeException {
        if (!existingEventType.getSchema().equals(eventType.getSchema())) {
            throw new InvalidEventTypeException("schema must not be changed");
        }
    }

    private void validateSchema(final EventType eventType) throws InvalidEventTypeException {
        try {
            final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

            final Schema schema = SchemaLoader.load(schemaAsJson);

            if (eventType.getCategory() == EventCategory.BUSINESS && schema.definesProperty("#/metadata")) {
                throw new InvalidEventTypeException("\"metadata\" property is reserved");
            }

            validatePartitionKeys(Optional.of(schema), eventType);

        } catch (final JSONException e) {
            throw new InvalidEventTypeException("schema must be a valid json");
        } catch (final SchemaException e) {
            throw new InvalidEventTypeException("schema must be a valid json-schema");
        }
    }

    private void validatePartitionKeys(final Optional<Schema> schemaO, final EventType eventType)
            throws InvalidEventTypeException, JSONException, SchemaException {
        if (!featureToggleService.isFeatureEnabled(CHECK_PARTITIONS_KEYS)) {
            return;
        }
        final Schema schema = schemaO.orElseGet(() -> {
            final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());
            return SchemaLoader.load(schemaAsJson);
        });


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
