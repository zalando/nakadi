package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EventValidationException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.InvalidPartitioningKeyFieldsException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.partitioning.PartitioningKeyFieldsPartitioningStrategy;
import de.zalando.aruha.nakadi.partitioning.PartitioningStrategy;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.validation.EventTypeValidator;
import de.zalando.aruha.nakadi.validation.ValidationError;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);

    private final TopicRepository topicRepository;
    private final EventTypeRepository eventTypeRepository;
    private final PartitioningStrategy partitioningKeyFieldsPartitioningStrategy = new PartitioningKeyFieldsPartitioningStrategy();
    private final EventTypeCache cache;

    public EventPublishingController(final TopicRepository topicRepository,
                                     final EventTypeRepository eventTypeRepository,
                                     final EventTypeCache cache) {
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.cache = cache;
    }

    @Timed(name = "post_events", absolute = true)
    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName, @RequestBody final String event,
                                    final NativeWebRequest nativeWebRequest) {
        LOG.trace("Received event {} for event type {}", event, eventTypeName);

        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);

            final JSONObject eventAsJson = parseJson(event);

            validateSchema(eventAsJson, eventType);

            String partitionId = applyPartitioningStrategy(eventType, eventAsJson);

            topicRepository.postEvent(eventTypeName, partitionId, event);
            return status(HttpStatus.CREATED).build();

        } catch (NoSuchEventTypeException e) {
            LOG.debug("Could not process event.", e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final EventValidationException e) {
            LOG.debug("Event validation error: {}", e.getValidationError().getMessage());
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.error("error posting to partition", e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    private String applyPartitioningStrategy(final EventType eventType, final JSONObject eventAsJson) throws InvalidPartitioningKeyFieldsException, NakadiException {
        String partitionId;
        if (!eventType.getPartitioningKeyFields().isEmpty()) {
            final List<String> partitions = topicRepository.listPartitionNames(eventType.getName());
            partitionId = partitioningKeyFieldsPartitioningStrategy.calculatePartition(eventType, eventAsJson, partitions);
        } else {
            // Will be replaced later:
            partitionId = "0";
        }
        return partitionId;
    }

    private void validateSchema(final JSONObject event, final EventType eventType) throws EventValidationException, InternalNakadiException {
        try {
            final EventTypeValidator validator = cache.getValidator(eventType.getName());
            final Optional<ValidationError> validationError = validator.validate(event);

            if (validationError.isPresent()) {
                throw new EventValidationException(validationError.get());
            }
        } catch (ExecutionException e) {
            LOG.error("Error loading validator", e);
            throw new InternalNakadiException("Error loading validator", e);
        }
    }

    private JSONObject parseJson(final String event) throws EventValidationException {
        try {
            return new JSONObject(event);
        } catch (JSONException e) {
            throw new EventValidationException(new ValidationError("payload must be a valid json"));
        }
    }
}
