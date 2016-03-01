package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicCreationException;
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

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeController.class);

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;

    @Autowired
    public EventTypeController(EventTypeRepository eventTypeRepository, TopicRepository topicRepository) {
        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        List<EventType> eventTypes = eventTypeRepository.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createEventType(@Valid @RequestBody final EventType eventType,
                                             final Errors errors,
                                             final NativeWebRequest nativeWebRequest) {
        validateSchema(eventType, errors);

        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        }

        try {
            eventTypeRepository.saveEventType(eventType);
            topicRepository.createTopic(eventType.getName());
            return status(HttpStatus.CREATED).build();
        } catch (DuplicatedEventTypeNameException e) {
            return create(e.asProblem(), nativeWebRequest);
        } catch (TopicCreationException e) {
            LOG.error("Problem creating kafka topic. Rolling back event type database registration.", e);

            try {
                eventTypeRepository.removeEventType(eventType.getName());
            } catch (NakadiException e1) {
                return create(e.asProblem(), nativeWebRequest);
            }
            return create(e.asProblem(), nativeWebRequest);
        } catch (NakadiException e) {
            LOG.error("Error creating event type " + eventType, e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventType eventType,
            final Errors errors,
            final NativeWebRequest nativeWebRequest) {
        try {
            validateUpdate(name, eventType, errors);

            if (!errors.hasErrors()) {
                eventTypeRepository.update(eventType);
                return status(HttpStatus.OK).build();
            } else {
                return create(new ValidationProblem(errors), nativeWebRequest);
            }
        } catch (NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", name);
            return create(e.asProblem(), nativeWebRequest);
        } catch (NakadiException e) {
            LOG.error("Unable to update event type", e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.GET)
    public ResponseEntity<?> exposeSingleEventType(@PathVariable final String name, final NativeWebRequest nativeWebRequest) {
        try {
            final EventType eventType = eventTypeRepository.findByName(name);
            return status(HttpStatus.OK).body(eventType);
        } catch (NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", name);
            return create(e.asProblem(), nativeWebRequest);
        } catch (InternalNakadiException e) {
            LOG.error("Problem loading event type " + name, e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    private void validateSchema(EventType eventType, Errors errors) {
        if (!errors.hasErrors()) {
            try {
                JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

                SchemaLoader.load(schemaAsJson);
            } catch (JSONException e) {
                errors.rejectValue("schema.schema", "", "must be a valid json");
            } catch (SchemaException e) {
                errors.rejectValue("schema.schema", "", "must be valid json-schema (http://json-schema.org)");
            }
        }
    }

    private void validateUpdate(final String name, final EventType eventType, final Errors errors) throws NoSuchEventTypeException, InternalNakadiException {
        if (!errors.hasErrors()) {
            final EventType existingEventType = eventTypeRepository.findByName(name);

            validateName(name, eventType, errors);
            validateSchema(eventType, existingEventType, errors);
        }
    }

    private void validateName(final String name, final EventType eventType, final Errors errors) {
        if (!eventType.getName().equals(name)) {
            errors.rejectValue("name", "",
                    "The submitted event type name \"" +
                            eventType.getName() +
                            "\" should match the parameter name \"" +
                            name + "\"");
        }
    }

    private void validateSchema(final EventType eventType, final EventType existingEventType, final Errors errors) {
        if (!existingEventType.getSchema().equals(eventType.getSchema())) {
            errors.rejectValue("schema", "", "The schema you've just submitted is different from the one in our system.");
        }
    }
}
