package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.managers.EventTypeManager;
import de.zalando.aruha.nakadi.managers.Result;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.security.Client;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
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
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import java.util.List;

import static de.zalando.aruha.nakadi.util.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_CREATION;
import static de.zalando.aruha.nakadi.util.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_DELETION;
import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeController.class);

    private final EventTypeManager manager;
    private final EventTypeRepository eventTypeRepository;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public EventTypeController(final EventTypeManager manager,
                               final EventTypeRepository eventTypeRepository,
                               final FeatureToggleService featureToggleService)
    {
        this.manager = manager;
        this.eventTypeRepository = eventTypeRepository;
        this.featureToggleService = featureToggleService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        final List<EventType> eventTypes = eventTypeRepository.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> create(@Valid @RequestBody final EventType eventType,
                                    final Errors errors,
                                    final NativeWebRequest request)
    {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_CREATION)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        Result<Void> result = manager.create(eventType);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.CREATED).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.DELETE)
    public ResponseEntity<?> delete(@PathVariable("name") final String eventTypeName,
                                    final NativeWebRequest request,
                                    @Client String clientId)
    {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_DELETION)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }

        Result<Void> result = manager.delete(eventTypeName, clientId);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventType eventType,
            final Errors errors,
            final NativeWebRequest request,
            @Client String clientId)
    {
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }
        Result<Void> update = manager.update(name, eventType, clientId);
        if (!update.isSuccessful()) {
            return Responses.create(update.getProblem(), request);
        }
        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> get(@PathVariable final String name, final NativeWebRequest request) {
        Result<EventType> result = manager.get(name);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.OK).body(result.getValue());
    }
}
