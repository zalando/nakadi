package org.zalando.nakadi.controller;

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
import org.springframework.web.util.UriComponents;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.WebResult;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.ws.rs.core.HttpHeaders;
import java.util.Set;

import static org.springframework.http.HttpStatus.NOT_IMPLEMENTED;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_OWNING_APPLICATION;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private final FeatureToggleService featureToggleService;
    private final ApplicationService applicationService;
    private final SubscriptionService subscriptionService;

    @Autowired
    public SubscriptionController(final FeatureToggleService featureToggleService,
                                  final ApplicationService applicationService,
                                  final SubscriptionService subscriptionService) {
        this.featureToggleService = featureToggleService;
        this.applicationService = applicationService;
        this.subscriptionService = subscriptionService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final SubscriptionBase subscriptionBase,
                                                     final Errors errors,
                                                     final NativeWebRequest request,
                                                     final Client client) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        if (featureToggleService.isFeatureEnabled(CHECK_OWNING_APPLICATION)
                && !applicationService.exists(subscriptionBase.getOwningApplication())) {
            return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                    "owning_application doesn't exist"), request);
        }
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        try {
            if (featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)) {
                try {
                    return ok(subscriptionService.getExistingSubscription(subscriptionBase));
                } catch (final NoSuchSubscriptionException e) {
                    return Responses.create(new ServiceUnavailableException(
                            "Subscription creation is temporarily unavailable", e).asProblem(), request);
                } catch (final NakadiException e) {
                    Responses.create(e.asProblem(), request);
                }
            }

            final Result<Subscription> result = subscriptionService.createSubscription(subscriptionBase, client);
            if (!result.isSuccessful()) {
                return Responses.create(result.getProblem(), request);
            }
            return prepareLocationResponse(result.getValue());
        } catch (final DuplicatedSubscriptionException e) {
            final Result<Subscription> result = subscriptionService.processDuplicatedSubscription(subscriptionBase);
            if (!result.isSuccessful()) {
                return Responses.create(result.getProblem(), request);
            }
            return ok(result.getValue());
        }
    }

    private ResponseEntity<?> ok(final Subscription existingSubscription) {
        final UriComponents location = subscriptionService.getSubscriptionUri(existingSubscription);
        return ResponseEntity.status(OK).location(location.toUri()).body(existingSubscription);
    }

    private ResponseEntity<?> prepareLocationResponse(final Subscription subscription) {
        final UriComponents location = subscriptionService.getSubscriptionUri(subscription);
        return ResponseEntity.status(HttpStatus.CREATED)
                .location(location.toUri())
                .header(HttpHeaders.CONTENT_LOCATION, location.toString())
                .body(subscription);
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> listSubscriptions(
            @Nullable @RequestParam(value = "owning_application", required = false) final String owningApplication,
            @Nullable @RequestParam(value = "event_type", required = false) final Set<String> eventTypes,
            @RequestParam(value = "limit", required = false, defaultValue = "20") final int limit,
            @RequestParam(value = "offset", required = false, defaultValue = "0") final int offset,
            final NativeWebRequest request) {

        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        return WebResult.wrap(() ->
                subscriptionService.listSubscriptions(owningApplication, eventTypes, limit, offset), request);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getSubscription(@PathVariable("id") final String subscriptionId,
                                             final NativeWebRequest request) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }

        return WebResult.wrap(() -> subscriptionService.getSubscription(subscriptionId), request);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteSubscription(@PathVariable("id") final String subscriptionId,
                                                final NativeWebRequest request, final Client client) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }

        return WebResult.wrap(() -> subscriptionService.deleteSubscription(subscriptionId, client), request,
                HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/{id}/stats", method = RequestMethod.GET)
    public ResponseEntity<?> getSubscriptionStats(@PathVariable("id") final String subscriptionId,
                                             final NativeWebRequest request) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }

        return WebResult.wrap(() -> subscriptionService.getSubscriptionStat(subscriptionId), request);
    }

}
