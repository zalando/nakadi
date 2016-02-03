package de.zalando.aruha.nakadi.controller;

import com.google.common.base.CaseFormat;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.problem.DuplicatedEventTypeNameProblem;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.everit.json.schema.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import java.net.URI;

import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/event_types")
public class EventTypeController {

    private EventTypeRepository repository;

    @Autowired
    public EventTypeController(EventTypeRepository repository) {
        this.repository = repository;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createEventType(@Valid @RequestBody final EventType eventType, Errors errors) throws Exception {
        if (errors.hasErrors()) {
            Problem problem = new ValidationProblem(errors);

            return status(HttpStatus.UNPROCESSABLE_ENTITY).body(problem);
        } else {
            return persist(eventType);
        }
    }

    private ResponseEntity<?> persist(EventType eventType) {
        try {
            repository.saveEventType(eventType);

            return status(HttpStatus.CREATED).build();
        } catch (DuplicatedEventTypeNameException e) {
            Problem problem = new DuplicatedEventTypeNameProblem(e.getName());

            return status(HttpStatus.CONFLICT).body(problem);
        } catch (NakadiException e) {
            return status(500).body(e.getMessage()); // TODO build proper Problem
        }
    }

    // TODO implement PUT

    // TODO implement GET list
}
