package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.problem.DuplicatedEventTypeNameProblem;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.NoSuchEventTypeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.*;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import javax.ws.rs.core.Response;
import java.util.List;

import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/event_types")
public class EventTypeController {

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
    public ResponseEntity<?> createEventType(@Valid @RequestBody final EventType eventType, final Errors errors) throws Exception {
        if (errors.hasErrors()) {
            return unprocessableEntity(errors);
        } else {
            return persist(eventType);
        }
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventType eventType,
            final Errors errors) {
        if (errors.hasErrors()) {
            return unprocessableEntity(errors);
        }

        try {
            final EventType existingEventType = repository.findByName(name);
            validateName(name, eventType, errors);
            validateSchema(eventType, existingEventType, errors);

            if (!errors.hasErrors()) {
                repository.update(eventType);
                return status(HttpStatus.OK).build();
            } else {
                return unprocessableEntity(errors);
            }
        } catch (NoSuchEventTypeException e) {
            final Problem problem = Problem.valueOf(Response.Status.NOT_FOUND);
            return status(HttpStatus.NOT_FOUND).body(problem);
        } catch (NakadiException e) {
            final Problem problem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, e.getMessage());
            return status(HttpStatus.UNPROCESSABLE_ENTITY).body(problem);
        }
    }

    private void validateName(final String name, final EventType eventType, final Errors errors) {
        if (!eventType.getName().equals(name)) {
            errors.rejectValue("name",
                    "The submitted event type name \"" +
                            eventType.getName() +
                            "\" should match the parameter name \"" +
                            name + "\"");
        }
    }

    private void validateSchema(final EventType eventType, final EventType existingEventType, final Errors errors) {
        if (!existingEventType.getEventTypeSchema().equals(eventType.getEventTypeSchema())) {
            errors.rejectValue("schema", "The schema you've just submitted is different from the one in our system.");
        }
    }

    private ResponseEntity<?> persist(final EventType eventType) {
        try {
            repository.saveEventType(eventType);
            return status(HttpStatus.CREATED).build();
        } catch (DuplicatedEventTypeNameException e) {
            final Problem problem = new DuplicatedEventTypeNameProblem(e.getName());

            return status(HttpStatus.CONFLICT).body(problem);
        } catch (NakadiException e) {
            return status(500).body(e.getMessage()); // TODO build proper Problem
        }
    }

    private ResponseEntity<?> unprocessableEntity(Errors errors) {
        final Problem problem = new ValidationProblem(errors);
        return status(HttpStatus.UNPROCESSABLE_ENTITY).body(problem);
    }
}
