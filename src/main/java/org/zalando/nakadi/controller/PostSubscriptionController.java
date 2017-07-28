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
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_OWNING_APPLICATION;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;


@RestController
public class PostSubscriptionController {

    private static final Logger LOG = LoggerFactory.getLogger(PostSubscriptionController.class);

    private final FeatureToggleService featureToggleService;
    private final ApplicationService applicationService;
    private final SubscriptionService subscriptionService;
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public PostSubscriptionController(final FeatureToggleService featureToggleService,
                                      final ApplicationService applicationService,
                                      final SubscriptionService subscriptionService,
                                      final AuthorizationValidator authorizationValidator) {
        this.featureToggleService = featureToggleService;
        this.applicationService = applicationService;
        this.subscriptionService = subscriptionService;
        this.authorizationValidator = authorizationValidator;
    }

    @RequestMapping(value = "/subscriptions", method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final SubscriptionBase subscriptionBase,
                                                     final Errors errors,
                                                     final NativeWebRequest request,
                                                     final Client client) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        authorizationValidator.authorizeSubscriptionRead(subscriptionBase);

        if (featureToggleService.isFeatureEnabled(CHECK_OWNING_APPLICATION)
                && !applicationService.exists(subscriptionBase.getOwningApplication())) {
            return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                    "owning_application doesn't exist"), request);
        }

        try {
            return ok(subscriptionService.getExistingSubscription(subscriptionBase));
        } catch (final NoSubscriptionException e) {
            if (featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)) {
                return Responses.create(Response.Status.SERVICE_UNAVAILABLE,
                        "Subscription creation is temporarily unavailable", request);
            }
            try {
                final Subscription subscription = subscriptionService.createSubscription(subscriptionBase, client);
                return prepareLocationResponse(subscription);
            } catch (final DuplicatedSubscriptionException ex) {
                throw new InconsistentStateException("Unexpected problem occurred when creating subscription", ex);
            }
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

    @ExceptionHandler({
            NoEventTypeException.class,
            WrongInitialCursorsException.class,
            TooManyPartitionsException.class})
    public ResponseEntity<Problem> handleUnprocessableSubscription(final MyNakadiRuntimeException1 exception,
                                                                   final NativeWebRequest request) {
        LOG.debug("Error occurred when working with subscriptions", exception);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAvailable(
            final FeatureNotAvailableException ex,
            final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(Problem.valueOf(NOT_IMPLEMENTED, ex.getMessage()), request);

    }
}
