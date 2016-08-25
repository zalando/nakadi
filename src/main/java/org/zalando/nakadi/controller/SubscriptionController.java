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
import org.zalando.nakadi.exceptions.ExceptionWrapper;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_IMPLEMENTED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_OWNING_APPLICATION;
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

    private final ApplicationService applicationService;

    @Autowired
    public SubscriptionController(final SubscriptionDbRepository subscriptionRepository,
                                  final EventTypeRepository eventTypeRepository,
                                  final FeatureToggleService featureToggleService,
                                  ApplicationService applicationService) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.featureToggleService = featureToggleService;
        this.applicationService = applicationService;
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
        if (featureToggleService.isFeatureEnabled(CHECK_OWNING_APPLICATION)
                && !applicationService.exists(subscriptionBase.getOwningApplication())) {
            return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                    "owning_application doesn't exist"), request);
        }

        final Map<String, Optional<EventType>> eventTypeMapping =
                subscriptionBase.getEventTypes().stream()
                        .collect(Collectors.toMap(Function.identity(),
                                        ExceptionWrapper.wrapFunction(eventTypeRepository::findByNameO)));
        final List<String> missingEventTypes = eventTypeMapping.entrySet().stream()
                .filter(entry -> !entry.getValue().isPresent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!missingEventTypes.isEmpty()) {
            return create(UNPROCESSABLE_ENTITY, createErrorMessage(missingEventTypes), request);
        }

        eventTypeMapping.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(eventType -> client.checkScopes(eventType.getReadScopes()));

        // generate subscription id and try to create subscription in DB
        final Subscription subscription = subscriptionRepository.createSubscription(subscriptionBase);
        return status(HttpStatus.CREATED).body(subscription);
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

}
