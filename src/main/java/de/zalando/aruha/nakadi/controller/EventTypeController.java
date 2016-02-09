package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.problem.DuplicatedEventTypeNameProblem;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.NoSuchEventTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
@RequestMapping(value = "/event_types")
public class EventTypeController {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeController.class);

    final private EventTypeRepository repository;

    @Autowired
    public EventTypeController(EventTypeRepository repository) {
        this.repository = repository;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        List<EventType> eventTypes = repository.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createEventType(@Valid @RequestBody final EventType eventType,
                                             final Errors errors,
                                             final NativeWebRequest nativeWebRequest) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        } else {
            return persist(eventType)
                    .map(problem -> (ResponseEntity) create(problem, nativeWebRequest))
                    .orElse(status(HttpStatus.CREATED).build());

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
                repository.update(eventType);
                return status(HttpStatus.OK).build();
            } else {
                return create(new ValidationProblem(errors), nativeWebRequest);
            }
        } catch (NoSuchEventTypeException e) {
            final Problem problem = Problem.valueOf(Response.Status.NOT_FOUND);
            return create(problem, nativeWebRequest);
        } catch (NakadiException e) {
            LOG.error("Unable to update event type", e);

            final Problem problem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, e.getMessage());
            return create(problem, nativeWebRequest);
        }
    }

    private void validateUpdate(final String name, final EventType eventType, final Errors errors) throws NakadiException {
        if (!errors.hasErrors()) {
            final EventType existingEventType = repository.findByName(name);

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

    private Optional<Problem> persist(final EventType eventType) {
        try {
            repository.saveEventType(eventType);
            return Optional.empty();
        } catch (DuplicatedEventTypeNameException e) {
            final Problem p = new DuplicatedEventTypeNameProblem(e.getName());
            return Optional.of(p);
        } catch (NakadiException e) {
            LOG.error("Error creating event type", e);

            return Optional.of(Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
