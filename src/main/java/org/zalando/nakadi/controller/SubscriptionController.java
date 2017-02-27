package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.WebResult;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.util.FeatureToggleService;

import javax.annotation.Nullable;
import java.util.Set;

import static org.springframework.http.HttpStatus.NOT_IMPLEMENTED;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

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
