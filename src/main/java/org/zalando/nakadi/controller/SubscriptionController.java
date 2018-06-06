package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidOffsetException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TimeLagStatsTimeoutException;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.service.subscription.SubscriptionService.StatsMode;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import java.util.Set;

import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.Status.REQUEST_TIMEOUT;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController implements NakadiProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionController.class);

    private final FeatureToggleService featureToggleService;
    private final SubscriptionService subscriptionService;

    @Autowired
    public SubscriptionController(final FeatureToggleService featureToggleService,
                                  final SubscriptionService subscriptionService) {
        this.featureToggleService = featureToggleService;
        this.subscriptionService = subscriptionService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> listSubscriptions(
            @Nullable @RequestParam(value = "owning_application", required = false) final String owningApplication,
            @Nullable @RequestParam(value = "event_type", required = false) final Set<String> eventTypes,
            @RequestParam(value = "show_status", required = false, defaultValue = "false") final boolean showStatus,
            @RequestParam(value = "limit", required = false, defaultValue = "20") final int limit,
            @RequestParam(value = "offset", required = false, defaultValue = "0") final int offset,
            final NativeWebRequest request) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        final PaginationWrapper<Subscription> wrapper = subscriptionService.listSubscriptions(owningApplication,
                eventTypes, showStatus, limit, offset);
        return status(OK).body(wrapper);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getSubscription(@PathVariable("id") final String subscriptionId,
                                             final NativeWebRequest request) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        final Subscription subscription = subscriptionService.getSubscription(subscriptionId);
        return status(OK).body(subscription);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteSubscription(@PathVariable("id") final String subscriptionId,
                                                final NativeWebRequest request) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        subscriptionService.deleteSubscription(subscriptionId);
        return status(NO_CONTENT).build();
    }

    @RequestMapping(value = "/{id}/stats", method = RequestMethod.GET)
    public ItemsWrapper<SubscriptionEventTypeStats> getSubscriptionStats(
            @PathVariable("id") final String subscriptionId,
            @RequestParam(value = "show_time_lag", required = false, defaultValue = "false") final boolean showTimeLag)
            throws InternalNakadiException, InconsistentStateException, ServiceTemporarilyUnavailableException, 
            Exception {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        final StatsMode statsMode = showTimeLag ? StatsMode.TIMELAG : StatsMode.NORMAL;
        try {
            return subscriptionService.getSubscriptionStat(subscriptionId, statsMode);
        } catch (final NakadiRuntimeException exception) {
            throw exception.getException();
        }
    }

    @ExceptionHandler(ErrorGettingCursorTimeLagException.class)
    public ResponseEntity<Problem> handleTimeLagException(final ErrorGettingCursorTimeLagException exception,
                                                          final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureTurnedOff(final FeatureNotAvailableException exception,
                                                          final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_IMPLEMENTED, exception.getMessage()), request);
    }

    @ExceptionHandler(InconsistentStateException.class)
    public ResponseEntity<Problem> handleInconsistentStateException(final InconsistentStateException exception,
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

    @ExceptionHandler(InvalidLimitException.class)
    public ResponseEntity<Problem> handleInvalidLimitException(final InvalidLimitException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidOffsetException.class)
    public ResponseEntity<Problem> handleInvalidOffsetException(final InvalidOffsetException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @Override
    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchSubscriptionException.class)
    public ResponseEntity<Problem> handleNoSuchSubscriptionException(final NoSuchSubscriptionException exception,
                                                                     final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @Override
    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TimeLagStatsTimeoutException.class)
    public ResponseEntity<Problem> handleTimeLagStatsTimeoutException(final TimeLagStatsTimeoutException exception,
                                                                      final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(REQUEST_TIMEOUT, exception.getMessage()), request);
    }

}
