package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.Collection;

@Service
public class EventStreamChecks {
    private final BlacklistService blacklistService;
    private final AuthorizationService authorizationService;
    private final SubscriptionCache subscriptionCache;
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamChecks.class);

    public EventStreamChecks(
            final BlacklistService blacklistService,
            final AuthorizationService authorizationService,
            final SubscriptionCache subscriptionCache) {
        this.blacklistService = blacklistService;
        this.authorizationService = authorizationService;
        this.subscriptionCache = subscriptionCache;
    }

    public boolean isConsumptionBlocked(final Collection<String> etNames, final String appId) {
        return blacklistService.isConsumptionBlocked(etNames, appId);
    }

    public boolean isConsumptionBlocked(final ConsumedEvent evt) {
        return !authorizationService.isAuthorized(AuthorizationService.Operation.READ, evt);
    }

    public boolean isSubscriptionConsumptionBlocked(final String subscriptionId, final String appId) {
        try {
            return blacklistService.isConsumptionBlocked(
                    subscriptionCache.getSubscription(subscriptionId).getEventTypes(), appId);
        } catch (final NoSuchSubscriptionException e) {
            // It's fine, subscription doesn't exists.
        } catch (final ServiceTemporarilyUnavailableException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

}
