package org.zalando.nakadi.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The caching works only inside the instance of the application, it does not sync cache across the instances, that's
 * why one should be careful about using it.
 */
@Service
public class SubscriptionCache {

    private final LoadingCache<String, Subscription> subscriptionsCache;

    @Autowired
    public SubscriptionCache(
            final SubscriptionDbRepository subscriptionRepository,
            @Value("${nakadi.cache.subscription.expireAfterAccessMs:300000}") final long expireAfterAccessMs) {
        this.subscriptionsCache = CacheBuilder.newBuilder()
                .expireAfterAccess(expireAfterAccessMs, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, Subscription>() {
                    @Override
                    public Subscription load(final String subscriptionId)
                            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
                        return subscriptionRepository.getSubscription(subscriptionId);
                    }
                });
    }

    /**
     * The method is not synced across Nakadi instances and eventually consistent in case of invalidation on
     * different instance, it may return stale data.
     *
     * @param subscriptionId
     * @return cached subscription or exception
     * @throws NoSuchSubscriptionException
     * @throws ServiceTemporarilyUnavailableException
     */
    public Subscription getSubscription(final String subscriptionId)
            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        try {
            return subscriptionsCache.get(subscriptionId);
        } catch (final UncheckedExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof NoSuchSubscriptionException) {
                throw (NoSuchSubscriptionException) cause;
            }
            throw new ServiceTemporarilyUnavailableException("Failed to access subscription cache", cause);
        } catch (final ExecutionException e) {
            throw new ServiceTemporarilyUnavailableException("Failed to access subscription cache", e.getCause());
        }
    }

    public void invalidateSubscription(final String subscriptionId) {
        subscriptionsCache.invalidate(subscriptionId);
    }
}
