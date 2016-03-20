package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.PartitionResolutionStrategy;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NoSuchPartitioningStrategyException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;

import javax.validation.Valid;
import java.util.List;

import static de.zalando.aruha.nakadi.domain.EventCategory.UNDEFINED;
import static de.zalando.aruha.nakadi.partitioning.PartitioningStrategy.HASH_STRATEGY;
import static de.zalando.aruha.nakadi.partitioning.PartitioningStrategy.USER_DEFINED_STRATEGY;
import static org.springframework.http.ResponseEntity.status;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeController.class);

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final PartitionResolver partitionResolver;

    @Autowired
    public EventTypeController(final EventTypeRepository eventTypeRepository, final TopicRepository topicRepository,
                               final PartitionResolver partitionResolver) {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.partitionResolver = partitionResolver;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        final List<EventType> eventTypes = eventTypeRepository.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createEventType(@Valid @RequestBody final EventType eventType,
                                             final Errors errors,
                                             final NativeWebRequest nativeWebRequest) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        }

        try {
            validateSchema(eventType);
            validatePartitioningStrategy(eventType);
            eventTypeRepository.saveEventType(eventType);
            topicRepository.createTopic(eventType.getName());
            return status(HttpStatus.CREATED).build();
        } catch (final InvalidEventTypeException | NoSuchPartitioningStrategyException |
                DuplicatedEventTypeNameException e) {
            LOG.debug("Failed to create EventType.", e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final TopicCreationException e) {
            LOG.error("Problem creating kafka topic. Rolling back event type database registration.", e);

            try {
                eventTypeRepository.removeEventType(eventType.getName());
            } catch (final NakadiException e1) {
                return create(e.asProblem(), nativeWebRequest);
            }
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.error("Error creating event type " + eventType, e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteEventType(@PathVariable("name") final String eventTypeName,
                                             final NativeWebRequest nativeWebRequest) {
        try {
            eventTypeRepository.removeEventType(eventTypeName);
            topicRepository.deleteTopic(eventTypeName);
            return status(HttpStatus.OK).build();
        } catch (final NoSuchEventTypeException e) {
            LOG.warn("Tried to remove EventType " + eventTypeName + " that doesn't exist", e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final TopicDeletionException e) {
            LOG.error("Problem deleting kafka topic " + eventTypeName, e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.error("Error deleting event type " + eventTypeName, e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventType eventType,
            final Errors errors,
            final NativeWebRequest nativeWebRequest) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        }

        try {
            validateUpdate(name, eventType);
            eventTypeRepository.update(eventType);
            return status(HttpStatus.OK).build();
        } catch (final InvalidEventTypeException e) {
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", name);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.error("Unable to update event type", e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> exposeSingleEventType(@PathVariable final String name, final NativeWebRequest nativeWebRequest) {
        try {
            final EventType eventType = eventTypeRepository.findByName(name);
            return status(HttpStatus.OK).body(eventType);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", name);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final InternalNakadiException e) {
            LOG.error("Problem loading event type " + name, e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    private void validatePartitioningStrategy(final EventType eventType) throws NoSuchPartitioningStrategyException,
            InvalidEventTypeException {
        final PartitionResolutionStrategy partitioningStrategy = eventType.getPartitionResolutionStrategy();
        if (!partitionResolver.strategyExists(partitioningStrategy.getName())) {
            throw new NoSuchPartitioningStrategyException("partitioning strategy does not exist: " +
                    partitioningStrategy.getName());
        }
        else if (HASH_STRATEGY.equals(partitioningStrategy.getName()) &&
                eventType.getPartitioningKeyFields().isEmpty()) {
            throw new InvalidEventTypeException("partitioning_key_fields field should be set for " +
                    "partition resolution strategy 'hash'");
        }
        else if (USER_DEFINED_STRATEGY.equals(partitioningStrategy.getName()) &&
                UNDEFINED.equals(eventType.getCategory())) {
            throw new InvalidEventTypeException("'user_defined' partition resolution strategy can't be used " +
                    "for EventType of category 'undefined'");
        }
    }

    private void validateSchema(final EventType eventType) throws InvalidEventTypeException {
        if (eventType.getSchema() != null) {
            try {
                final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

                if (hasReservedField(eventType, schemaAsJson, "metadata")) {
                    throw new InvalidEventTypeException("\"metadata\" property is reserved");
                }

                SchemaLoader.load(schemaAsJson);
            } catch (JSONException e) {
                throw new InvalidEventTypeException("schema must be a valid json");
            } catch (SchemaException e) {
                throw new InvalidEventTypeException("schema must be a valid json-schema");
            }
        }
    }

    private boolean hasReservedField(final EventType eventType, final JSONObject schemaAsJson, final String field) {
        return eventType.getCategory() == EventCategory.BUSINESS
                && schemaAsJson.optJSONObject("properties") != null
                && schemaAsJson.getJSONObject("properties").has(field);
    }

    private void validateUpdate(final String name, final EventType eventType) throws NoSuchEventTypeException, InternalNakadiException, InvalidEventTypeException {
        final EventType existingEventType = eventTypeRepository.findByName(name);

        validateName(name, eventType);
        validateSchemaChange(eventType, existingEventType);
    }

    private void validateName(final String name, final EventType eventType) throws InvalidEventTypeException {
        if (!eventType.getName().equals(name)) {
            throw new InvalidEventTypeException("path does not match resource name");
        }
    }

    private void validateSchemaChange(final EventType eventType, final EventType existingEventType) throws InvalidEventTypeException {
        final boolean existingNonNullSchemaChanged =
                existingEventType.getSchema() != null && !existingEventType.getSchema().equals(eventType.getSchema());

        final boolean existingNullSchemaChanged =
                existingEventType.getSchema() == null && eventType.getSchema() != null;


        if (existingNonNullSchemaChanged || existingNullSchemaChanged) {
            throw new InvalidEventTypeException("schema must not be changed");
        }
    }
}
