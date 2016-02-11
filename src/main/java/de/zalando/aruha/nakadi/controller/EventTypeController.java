package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.problem.DuplicatedEventTypeNameProblem;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.TopicCreationException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
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
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
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
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        }

        try {
            eventTypeRepository.saveEventType(eventType);
            topicRepository.createTopic(eventType.getName());
            return status(HttpStatus.CREATED).build();
        } catch (DuplicatedEventTypeNameException e) {
            final Problem problem = new DuplicatedEventTypeNameProblem(e.getName());
            return create(problem, nativeWebRequest);
        } catch (TopicCreationException e) {
            LOG.error("Problem creating kafka topic. Rolling back event type database registration.", e);

            eventTypeRepository.removeEventType(eventType.getName());
            Problem problem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);
            return create(problem, nativeWebRequest);
        } catch (NakadiException e) {
            LOG.error("Error creating event type", e);

            Problem problem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);
            return create(problem, nativeWebRequest);
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
            final Problem problem = Problem.valueOf(NOT_FOUND);
            return create(problem, nativeWebRequest);
        } catch (NakadiException e) {
            LOG.error("Unable to update event type", e);

            final Problem problem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, e.getMessage());
            return create(problem, nativeWebRequest);
        }
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.GET)
    public ResponseEntity<?> exposeSingleEventType(@PathVariable final String name, final NativeWebRequest nativeWebRequest) {
        try {
            final EventType eventType = eventTypeRepository.findByName(name);
            return status(HttpStatus.OK).body(eventType);
        } catch (NoSuchEventTypeException e) {
            LOG.debug("Could not find EventType: {}", name);
            return create(Problem.valueOf(NOT_FOUND, "EventType '" + name + "' does not exist."), nativeWebRequest);
        }
    }

    private void validateUpdate(final String name, final EventType eventType, final Errors errors) throws NakadiException {
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
        if (!existingEventType.getEventTypeSchema().equals(eventType.getEventTypeSchema())) {
            errors.rejectValue("eventTypeSchema", "", "The schema you've just submitted is different from the one in our system.");
        }
    }
}
