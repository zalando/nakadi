package org.zalando.nakadi.controller;

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
import org.springframework.web.util.UriComponents;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionUpdateConflictException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableSubscriptionException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.problem.Problem;

import javax.validation.Valid;

import static org.apache.http.HttpHeaders.CONTENT_LOCATION;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;


@RestController
public class PostSubscriptionController extends NakadiProblemControllerAdvice {

    private static final Logger LOG = LoggerFactory.getLogger(PostSubscriptionController.class);

    private final FeatureToggleService featureToggleService;
    private final ApplicationService applicationService;
    private final SubscriptionService subscriptionService;

    @Autowired
    public PostSubscriptionController(final FeatureToggleService featureToggleService,
                                      final ApplicationService applicationService,
                                      final SubscriptionService subscriptionService) {
        this.featureToggleService = featureToggleService;
        this.applicationService = applicationService;
        this.subscriptionService = subscriptionService;
    }

    @RequestMapping(value = "/subscriptions", method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final SubscriptionBase subscriptionBase,
                                                     final Errors errors,
                                                     final NativeWebRequest request) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }

        try {
            return ok(subscriptionService.getExistingSubscription(subscriptionBase));
        } catch (final NoSuchSubscriptionException e) {
            if (featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)) {
                return create(Problem.valueOf(SERVICE_UNAVAILABLE,
                        "Subscription creation is temporarily unavailable"), request);
            }
            try {
                final Subscription subscription = subscriptionService.createSubscription(subscriptionBase);
                return prepareLocationResponse(subscription);
            } catch (final DuplicatedSubscriptionException ex) {
                throw new InconsistentStateException("Unexpected problem occurred when creating subscription", ex);
            } catch (final NoSuchEventTypeException ex) {
                throw new UnprocessableSubscriptionException(ex.getMessage());
            }
        }
    }

    @RequestMapping(value = "/subscriptions/{subscription_id}", method = RequestMethod.PUT)
    public ResponseEntity<?> updateSubscription(
            @PathVariable("subscription_id") final String subscriptionId,
            @Valid @RequestBody final SubscriptionBase subscription,
            final Errors errors,
            final NativeWebRequest request) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }
        try {
            subscriptionService.updateSubscription(subscriptionId, subscription);
            return ResponseEntity.noContent().build();
        } catch (final SubscriptionUpdateConflictException ex) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, ex.getMessage()), request);
        } catch (final NoSuchSubscriptionException ex) {
            return create(Problem.valueOf(NOT_FOUND, ex.getMessage()), request);
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
                .header(CONTENT_LOCATION, location.toString())
                .body(subscription);
    }
}
