package org.zalando.nakadi.controller;

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
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.NakadiWrapperException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import java.util.List;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.CHECK_OWNING_APPLICATION;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_CREATION;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_DELETION;
import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController implements NakadiProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeController.class);

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

        if (featureToggleService.isFeatureEnabled(CHECK_OWNING_APPLICATION)
                && !applicationService.exists(eventType.getOwningApplication())) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY,
                    "owning_application doesn't exist"), request);
        }

        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }

        eventTypeService.create(eventType);

        return ResponseEntity.status(HttpStatus.CREATED).headers(generateWarningHeaders()).build();
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
            NakadiWrapperException,
            ServiceTemporarilyUnavailableException,
            UnableProcessException {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }

        eventTypeService.update(name, eventType);

        return status(HttpStatus.OK).headers(generateWarningHeaders()).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> get(@PathVariable final String name, final NativeWebRequest request) {
        return status(HttpStatus.OK).body(eventTypeService.get(name));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Problem> handleAccessDeniedException(final AccessDeniedException exception,
                                                               final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.explain()), request);
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<Problem> handleConflictException(final ConflictException exception,
                                                           final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(DuplicatedEventTypeNameException.class)
    public ResponseEntity<Problem> handleDuplicatedEventTypeNameException(
            final DuplicatedEventTypeNameException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeDeletionException.class)
    public ResponseEntity<Problem> handleEventTypeDeletionException(final EventTypeDeletionException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeOptionsValidationException.class)
    public ResponseEntity<Problem> handleEventTypeOptionsValidationException(
            final EventTypeOptionsValidationException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeUnavailableException.class)
    public ResponseEntity<Problem> handleEventTypeUnavailableException(final EventTypeUnavailableException exception,
                                                                       final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(InternalNakadiException.class)
    public ResponseEntity<Problem> handleInternalNakadiException(final InternalNakadiException exception,
                                                                 final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidEventTypeException.class)
    public ResponseEntity<Problem> handleInvalidEventTypeException(final InvalidEventTypeException exception,
                                                                   final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @Override
    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchPartitionStrategyException.class)
    public ResponseEntity<Problem> handleNoSuchPartitionStrategyException(
            final NoSuchPartitionStrategyException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception,
            final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TopicCreationException.class)
    public ResponseEntity<Problem> handleTopicCreationException(final TopicCreationException exception,
                                                                final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(UnprocessableEntityException.class)
    public ResponseEntity<Problem> handleUnprocessableEntityException(final UnprocessableEntityException exception,
                                                                      final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    private HttpHeaders generateWarningHeaders() {
        final HttpHeaders headers = new HttpHeaders();
        if (!nakadiSettings.getWarnAllDataAccessMessage().isEmpty()) {
            headers.add(HttpHeaders.WARNING,
                    String.format("299 nakadi \"%s\"", nakadiSettings.getWarnAllDataAccessMessage()));
        }
        return headers;
    }
}
