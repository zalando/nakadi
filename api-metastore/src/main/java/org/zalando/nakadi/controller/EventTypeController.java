package org.zalando.nakadi.controller;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
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
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.exceptions.runtime.InvalidOwningApplicationException;
import org.zalando.nakadi.model.AuthorizationAttributeQueryParser;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.domain.Feature.DISABLE_EVENT_TYPE_CREATION;
import static org.zalando.nakadi.domain.Feature.DISABLE_EVENT_TYPE_DELETION;
import static org.zalando.nakadi.domain.Feature.RETURN_BODY_ON_CREATE_UPDATE_EVENT_TYPE;

@RestController
@RequestMapping(value = "/event-types")
public class EventTypeController {

    private final EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService;
    private final AdminService adminService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public EventTypeController(final EventTypeService eventTypeService,
                               final FeatureToggleService featureToggleService,
                               final AdminService adminService,
                               final NakadiSettings nakadiSettings) {
        this.eventTypeService = eventTypeService;
        this.featureToggleService = featureToggleService;
        this.adminService = adminService;
        this.nakadiSettings = nakadiSettings;
    }


    @InitBinder
    public void initBinder(final WebDataBinder binder) {
        binder.registerCustomEditor(AuthorizationAttribute.class, new AuthorizationAttributeQueryParser());
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> list(
            @Nullable @RequestParam final AuthorizationAttribute writer,
            @Nullable @RequestParam(value = "owning_application", required = false) final String owningApplication
    ) {
        return status(HttpStatus.OK).body(eventTypeService.list(writer, owningApplication));
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> create(@Valid @RequestBody final EventTypeBase eventType,
                                    final Errors errors)
            throws TopicCreationException, InternalNakadiException, NoSuchPartitionStrategyException,
            DuplicatedEventTypeNameException, InvalidEventTypeException, ValidationException,
            FeatureNotAvailableException, InvalidOwningApplicationException {
        if (featureToggleService.isFeatureEnabled(DISABLE_EVENT_TYPE_CREATION)) {
            throw new FeatureNotAvailableException("Event Type creation is disabled", DISABLE_EVENT_TYPE_CREATION);
        }

        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }

        eventTypeService.create(eventType, true);

        final ResponseEntity.BodyBuilder bodyBuilder = status(HttpStatus.CREATED)
                .headers(generateWarningHeaders(eventType));
        if (featureToggleService.isFeatureEnabled(RETURN_BODY_ON_CREATE_UPDATE_EVENT_TYPE)) {
            return bodyBuilder.body(eventTypeService.get(eventType.getName()));
        } else {
            return bodyBuilder.build();
        }
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
            throw new ForbiddenOperationException("Event Type deletion is disabled");
        }

        eventTypeService.delete(eventTypeName);

        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.PUT)
    public ResponseEntity<?> update(
            @PathVariable("name") final String name,
            @RequestBody @Valid final EventTypeBase eventType,
            final Errors errors)
            throws TopicConfigException,
            InconsistentStateException,
            NakadiRuntimeException,
            ServiceTemporarilyUnavailableException,
            UnableProcessException,
            NoSuchPartitionStrategyException,
            ValidationException,
            InvalidOwningApplicationException {
        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }

        eventTypeService.update(name, eventType);

        final ResponseEntity.BodyBuilder bodyBuilder = status(HttpStatus.OK)
                .headers(generateWarningHeaders(eventType));
        if (featureToggleService.isFeatureEnabled(RETURN_BODY_ON_CREATE_UPDATE_EVENT_TYPE)) {
            return bodyBuilder.body(eventTypeService.get(eventType.getName()));
        } else {
            return bodyBuilder.build();
        }
    }

    @RequestMapping(value = "/{name:.+}", method = RequestMethod.GET)
    public ResponseEntity<?> get(@PathVariable final String name)
            throws NoSuchEventTypeException, InternalNakadiException {
        final EventType eventType = eventTypeService.get(name);
        return status(HttpStatus.OK).body(eventType);
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
}
