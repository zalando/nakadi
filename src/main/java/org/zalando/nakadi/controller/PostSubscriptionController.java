package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.util.UriComponents;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.problem.Problem;

import javax.validation.Valid;

import static org.springframework.http.HttpHeaders.CONTENT_LOCATION;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.CHECK_OWNING_APPLICATION;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;


@RestController
public class PostSubscriptionController implements NakadiProblemHandling {

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
                                                     final NativeWebRequest request,
                                                     final Client client) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), request);
        }

        if (featureToggleService.isFeatureEnabled(CHECK_OWNING_APPLICATION)
                && !applicationService.exists(subscriptionBase.getOwningApplication())) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY,
                    "owning_application doesn't exist"), request);
        }

        try {
            return ok(subscriptionService.getExistingSubscription(subscriptionBase));
        } catch (final NoSubscriptionException e) {
            if (featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)) {
                return create(Problem.valueOf(SERVICE_UNAVAILABLE,
                        "Subscription creation is temporarily unavailable"), request);
            }
            try {
                final Subscription subscription = subscriptionService.createSubscription(subscriptionBase);
                return prepareLocationResponse(subscription);
            } catch (final DuplicatedSubscriptionException ex) {
                throw new InconsistentStateException("Unexpected problem occurred when creating subscription", ex);
            }
        }
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAvailableException(
            final FeatureNotAvailableException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_IMPLEMENTED, exception.getMessage()), request);

    }

    @Override
    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                   final NativeWebRequest request) {
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler({
            WrongInitialCursorsException.class,
            TooManyPartitionsException.class})
    public ResponseEntity<Problem> handleUnprocessableSubscription(final NakadiRuntimeBaseException exception,
                                                                   final NativeWebRequest request) {
        LOG.debug("Error occurred when working with subscriptions", exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
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
