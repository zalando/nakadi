package org.zalando.nakadi.controller;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.Result;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_CREATION;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_DELETION;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private static final Logger LOG = LoggerFactory.getLogger(TimelinesController.class);

    private final EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService;
    private final ApplicationService applicationService;
    private final AdminService adminService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public EventTypeController(final EventTypeService eventTypeService,
                               final FeatureToggleService featureToggleService,
                               final ApplicationService applicationService,
                               final AdminService adminService,
                               final NakadiSettings nakadiSettings) {
        this.eventTypeService = eventTypeService;
        this.featureToggleService = featureToggleService;
        this.applicationService = applicationService;
        this.adminService = adminService;
        this.nakadiSettings = nakadiSettings;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list() {
        final List<EventType> eventTypes = eventTypeService.list();

        return status(HttpStatus.OK).body(eventTypes);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> create(@Valid @RequestBody final EventTypeBase eventType,
                                    final Errors errors,
                                    final NativeWebRequest request)
            throws TopicCreationException, InternalNakadiException, NoSuchPartitionStrategyException,
            DuplicatedEventTypeNameException, InvalidEventTypeException {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_CREATION)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }

        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        eventTypeService.create(eventType);

        return ResponseEntity.status(HttpStatus.CREATED).headers(generateWarningHeaders(eventType)).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.DELETE)
    public ResponseEntity<?> delete(@PathVariable("name") final String eventTypeName)
            throws EventTypeDeletionException,
            AccessDeniedException,
            NoSuchEventTypeException,
            ConflictException,
            ServiceTemporarilyUnavailableException {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_DELETION)
                && !adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }

        eventTypeService.delete(eventTypeName);

        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventTypeBase eventType,
            final Errors errors,
            final NativeWebRequest request)
            throws TopicConfigException,
            InconsistentStateException,
            NakadiRuntimeException,
            ServiceTemporarilyUnavailableException,
            UnableProcessException,
            NoSuchPartitionStrategyException{
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        eventTypeService.update(name, eventType);

        return status(HttpStatus.OK).headers(generateWarningHeaders(eventType)).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> get(@PathVariable final String name, final NativeWebRequest request) {
        final Result<EventType> result = eventTypeService.get(name);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(HttpStatus.OK).body(result.getValue());
    }

    private HttpHeaders generateWarningHeaders(final EventTypeBase eventType) {
        final HttpHeaders headers = new HttpHeaders();

        final List<String> warnings = Lists.newArrayList(nakadiSettings.getWarnAllDataAccessMessage());

        if (eventType.getCleanupPolicy().equals(CleanupPolicy.COMPACT)) {
            warnings.add(nakadiSettings.getLogCompactionWarnMessage());
        }

        final String warningMessage = warnings.stream()
                .filter(s -> !s.isEmpty()).collect(Collectors.joining(". "));

        if (!warnings.isEmpty()) {
            headers.add(HttpHeaders.WARNING,
                    String.format("299 nakadi \"%s\"", warningMessage));
        }

        return headers;
    }

    @ExceptionHandler(EventTypeDeletionException.class)
    public ResponseEntity<Problem> deletion(final EventTypeDeletionException exception,
                                            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Response.Status.INTERNAL_SERVER_ERROR, exception.getMessage(), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> unableProcess(final UnableProcessException exception,
                                                 final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<Problem> conflict(final ConflictException exception, final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Response.Status.CONFLICT, exception.getMessage(), request);
    }

    @ExceptionHandler(EventTypeUnavailableException.class)
    public ResponseEntity<Problem> eventTypeUnavailable(final EventTypeUnavailableException exception,
                                                        final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(NoSuchPartitionStrategyException.class)
    public ResponseEntity<Problem> noSuchPartitionStrategyException(final NoSuchPartitionStrategyException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(DuplicatedEventTypeNameException.class)
    public ResponseEntity<Problem> duplicatedEventTypeNameException(final DuplicatedEventTypeNameException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Problem.valueOf(Response.Status.CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidEventTypeException.class)
    public ResponseEntity<Problem> invalidEventTypeException(final InvalidEventTypeException exception,
                                                             final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeOptionsValidationException.class)
    public ResponseEntity<Problem> unableProcess(final EventTypeOptionsValidationException exception,
                                                 final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }
}
