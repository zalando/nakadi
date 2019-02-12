package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.service.subscription.SubscriptionService.StatsMode;

import javax.annotation.Nullable;
import java.util.Set;

import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private final SubscriptionService subscriptionService;

    @Autowired
    public SubscriptionController(final SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> listSubscriptions(
            @Nullable @RequestParam(value = "owning_application", required = false) final String owningApplication,
            @Nullable @RequestParam(value = "event_type", required = false) final Set<String> eventTypes,
            @RequestParam(value = "show_status", required = false, defaultValue = "false") final boolean showStatus,
            @RequestParam(value = "limit", required = false, defaultValue = "20") final int limit,
            @RequestParam(value = "offset", required = false, defaultValue = "0") final int offset,
            final NativeWebRequest request)
            throws InvalidLimitException, ServiceTemporarilyUnavailableException {
        return status(OK)
                .body(subscriptionService
                        .listSubscriptions(owningApplication, eventTypes, showStatus, limit, offset));
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getSubscription(@PathVariable("id") final String subscriptionId,
                                             final NativeWebRequest request)
            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        return status(OK).body(subscriptionService.getSubscription(subscriptionId));
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteSubscription(@PathVariable("id") final String subscriptionId,
                                                final NativeWebRequest request)
            throws DbWriteOperationsBlockedException, NoSuchSubscriptionException, NoSuchEventTypeException,
            ServiceTemporarilyUnavailableException, InternalNakadiException {
        subscriptionService.deleteSubscription(subscriptionId);
        return status(NO_CONTENT).build();
    }

    @RequestMapping(value = "/{id}/stats", method = RequestMethod.GET)
    public ItemsWrapper<SubscriptionEventTypeStats> getSubscriptionStats(
            @PathVariable("id") final String subscriptionId,
            @RequestParam(value = "show_time_lag", required = false, defaultValue = "false") final boolean showTimeLag)
            throws InconsistentStateException,
            NoSuchEventTypeException, NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        final StatsMode statsMode = showTimeLag ? StatsMode.TIMELAG : StatsMode.NORMAL;
        return subscriptionService.getSubscriptionStat(subscriptionId, statsMode);
    }
}
