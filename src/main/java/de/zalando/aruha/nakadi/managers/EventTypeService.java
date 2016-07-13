package de.zalando.aruha.nakadi.managers;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeStatistics;
import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NoSuchPartitionStrategyException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.security.Client;
import de.zalando.aruha.nakadi.util.UUIDGenerator;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Objects;
import java.util.Optional;

public class EventTypeService {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeService.class);

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final UUIDGenerator uuidGenerator;

    @Autowired
    public EventTypeService(final EventTypeRepository eventTypeRepository, final TopicRepository topicRepository,
                            final PartitionResolver partitionResolver, final Enrichment enrichment,
                            final UUIDGenerator uuidGenerator)
    {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.uuidGenerator = uuidGenerator;
    }

    public Result<Void> create(final EventType eventType) {
        try {
            assignTopic(eventType);
            validateSchema(eventType);
            enrichment.validate(eventType);
            partitionResolver.validate(eventType);
            eventTypeRepository.saveEventType(eventType);
            topicRepository.createTopic(eventType.getTopic(), eventType.getDefaultStatistics());
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
            } else if (!client.is(eventType.get().getOwningApplication())) {
                return Result.forbidden("You don't have access to this event type");
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
            if (!client.is(original.getOwningApplication())) {
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
        validateSchemaChange(eventType, existingEventType);
        eventType.setDefaultStatistics(
                validateStatisticsUpdate(existingEventType.getDefaultStatistics(), eventType.getDefaultStatistics()));
    }

    private EventTypeStatistics validateStatisticsUpdate(final EventTypeStatistics existing, final EventTypeStatistics newStatistics) throws InvalidEventTypeException {
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

    private void validateSchemaChange(final EventType eventType, final EventType existingEventType) throws InvalidEventTypeException {
        if (!existingEventType.getSchema().equals(eventType.getSchema())) {
            throw new InvalidEventTypeException("schema must not be changed");
        }
    }


    private void validateSchema(final EventType eventType) throws InvalidEventTypeException {
        try {
            final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

            if (hasReservedField(eventType, schemaAsJson, "metadata")) {
                throw new InvalidEventTypeException("\"metadata\" property is reserved");
            }

            SchemaLoader.load(schemaAsJson);
        } catch (final JSONException e) {
            throw new InvalidEventTypeException("schema must be a valid json");
        } catch (final SchemaException e) {
            throw new InvalidEventTypeException("schema must be a valid json-schema");
        }
    }

    private void assignTopic(final EventType eventType) {
        eventType.setTopic(uuidGenerator.randomUUID().toString());
    }

    private boolean hasReservedField(final EventType eventType, final JSONObject schemaAsJson, final String field) {
        return eventType.getCategory() == EventCategory.BUSINESS
                && schemaAsJson.optJSONObject("properties") != null
                && schemaAsJson.getJSONObject("properties").has(field);
    }

}
