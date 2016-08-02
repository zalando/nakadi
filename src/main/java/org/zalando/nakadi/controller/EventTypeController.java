package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.validation.EventTypeOptionsValidator;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import java.util.List;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_CREATION;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_DELETION;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private final EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService;
    private final EventTypeOptionsValidator eventTypeOptionsValidator;
    private final ApplicationService applicationService;

    @Autowired
    public EventTypeController(final EventTypeService eventTypeService,
                               final FeatureToggleService featureToggleService,
                               final EventTypeOptionsValidator eventTypeOptionsValidator,
                               final ApplicationService applicationService) {
        this.eventTypeService = eventTypeService;
        this.featureToggleService = featureToggleService;
        this.eventTypeOptionsValidator = eventTypeOptionsValidator;
        this.applicationService = applicationService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        final List<EventType> eventTypes = eventTypeService.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> create(@Valid @RequestBody final EventType eventType,
                                    final Errors errors,
                                    final NativeWebRequest request) {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_CREATION)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }

        ValidationUtils.invokeValidator(eventTypeOptionsValidator, eventType.getOptions(), errors);
        if (!applicationService.exists(eventType.getOwningApplication())) {
            return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, "owning_application doesn't exist"), request);
        }


        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        final Result<Void> result = eventTypeService.create(eventType);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.CREATED).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.DELETE)
    public ResponseEntity<?> delete(@PathVariable("name") final String eventTypeName,
                                    final NativeWebRequest request,
                                    final Client client) {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_DELETION)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }

        final Result<Void> result = eventTypeService.delete(eventTypeName, client);
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
            final Client client) {
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }
        final Result<Void> update = eventTypeService.update(name, eventType, client);
        if (!update.isSuccessful()) {
            return Responses.create(update.getProblem(), request);
        }
        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> get(@PathVariable final String name, final NativeWebRequest request) {
        final Result<EventType> result = eventTypeService.get(name);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.OK).body(result.getValue());
    }

}
