package org.zalando.nakadi.controller;

import org.apache.commons.lang3.StringUtils;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionListWrapper;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.util.FeatureToggleService;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_IMPLEMENTED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.problem.spring.web.advice.Responses.create;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionController.class);

    private final SubscriptionDbRepository subscriptionRepository;

    private final EventTypeRepository eventTypeRepository;

    private final FeatureToggleService featureToggleService;

    @Autowired
    public SubscriptionController(final SubscriptionDbRepository subscriptionRepository,
                                  final EventTypeRepository eventTypeRepository,
                                  final FeatureToggleService featureToggleService) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.featureToggleService = featureToggleService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final SubscriptionBase subscriptionBase,
                                                     final Errors errors,
                                                     final NativeWebRequest request,
                                                     final Client client) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }

        try {
            return createSubscription(subscriptionBase, request, client);
        } catch (final DuplicatedSubscriptionException e) {
            try {
                return new ResponseEntity<>(getExistingSubscription(subscriptionBase), HttpStatus.OK);
            } catch (final ServiceUnavailableException ex) {
                LOG.error("Error occurred during fetching existing subscription", ex);
                return create(e.asProblem(), request);
            } catch (final NoSuchSubscriptionException | InternalNakadiException ex) {
                LOG.error("Error occurred during fetching existing subscription", ex);
                return create(INTERNAL_SERVER_ERROR, ex.getProblemMessage(), request);
            }
        } catch (final NakadiException e) {
            LOG.error("Error occurred during subscription creation", e);
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> listSubscriptions(
            @Nullable @RequestParam(value = "owning_application", required = false) final String owningApplication,
            final NativeWebRequest request) {

        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }

        try {
            final List<Subscription> subscriptions = owningApplication == null ?
                    subscriptionRepository.listSubscriptions() :
                    subscriptionRepository.listSubscriptionsForOwningApplication(owningApplication);
            return status(OK).body(new SubscriptionListWrapper(subscriptions));

        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred during listing of subscriptions", e);
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getSubscription(@PathVariable("id") final String subscriptionId,
                                             final NativeWebRequest request) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            return status(OK).body(subscription);
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription: " + subscriptionId, e);
            return create(e.asProblem(), request);
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to get subscription: " + subscriptionId, e);
            return create(e.asProblem(), request);
        }
    }

    private ResponseEntity<?> createSubscription(final SubscriptionBase subscriptionBase,
                                                 final NativeWebRequest request,
                                                 final Client client)
            throws InternalNakadiException, DuplicatedSubscriptionException, ServiceUnavailableException {
        final EventTypesHolder eventTypesHolder = getEventTypesHolder(subscriptionBase);
        if (eventTypesHolder.hasMissingEvents()) {
            final String errorMessage = createErrorMessage(eventTypesHolder.getMissingEventTypes());
            return create(UNPROCESSABLE_ENTITY, errorMessage, request);
        }
        eventTypesHolder.getEventTypes().stream().forEach(eventType -> client.checkScopes(eventType.getReadScopes()));

        // generate subscription id and try to create subscription in DB
        final Subscription subscription = subscriptionRepository.createSubscription(subscriptionBase);
        return status(HttpStatus.CREATED).body(subscription);
    }

    private EventTypesHolder getEventTypesHolder(final SubscriptionBase subscriptionBase)
            throws InternalNakadiException {
        final EventTypesHolder eventTypesHolder = new EventTypesHolder();
        for (final String etName : subscriptionBase.getEventTypes()) {
            try {
                eventTypesHolder.addEventType(eventTypeRepository.findByName(etName));
            } catch (NoSuchEventTypeException e) {
                eventTypesHolder.addMissingEventType(etName);
            }
        }
        return eventTypesHolder;
    }

    private String createErrorMessage(final List<String> missingEventTypes) {
        return new StringBuilder()
                .append("Failed to create subscription, event type(s) not found: '")
                .append(StringUtils.join(missingEventTypes, "','"))
                .append("'").toString();
    }

    private Subscription getExistingSubscription(final SubscriptionBase subscriptionBase)
            throws NoSuchSubscriptionException, InternalNakadiException, ServiceUnavailableException {
        return subscriptionRepository.getSubscription(
                subscriptionBase.getOwningApplication(),
                subscriptionBase.getEventTypes(),
                subscriptionBase.getConsumerGroup());
    }

    private final class EventTypesHolder {
        private List<EventType> eventTypes = Collections.emptyList();
        private List<String> missingEventTypes = Collections.emptyList();

        private void addEventType(final EventType eventType) {
            if (eventTypes == Collections.<EventType>emptyList()) {
                eventTypes = new LinkedList<>();
            }
            eventTypes.add(eventType);
        }

        private void addMissingEventType(final String etName) {
            if (missingEventTypes == Collections.<String>emptyList()) {
                missingEventTypes = new LinkedList<>();
            }
            missingEventTypes.add(etName);
        }

        private List<EventType> getEventTypes() {
            return eventTypes;
        }

        private List<String> getMissingEventTypes() {
            return missingEventTypes;
        }

        private boolean hasMissingEvents() {
            return !missingEventTypes.isEmpty();
        }
    }

}
