package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.concurrent.TimeUnit;

@Service
public class SubscriptionClientFactory {

    private final ZooKeeperHolder zkHolder;
    private final ObjectMapper objectMapper;
    private final long maxCommitTimeoutMs;

    @Autowired
    public SubscriptionClientFactory(
            final ZooKeeperHolder zkHolder,
            final ObjectMapper objectMapper,
            final NakadiSettings nakadiSettings) {
        this.zkHolder = zkHolder;
        this.objectMapper = objectMapper;
        this.maxCommitTimeoutMs = TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout());
    }

    public ZkSubscriptionClient createClient(final Subscription subscription)
            throws InternalNakadiException, NoSuchEventTypeException, ZookeeperException {
        return createClient(subscription, maxCommitTimeoutMs);
    }

    public ZkSubscriptionClient createClient(final Subscription subscription,
                                             final long commitTimeoutMillis)
            throws InternalNakadiException, NoSuchEventTypeException, ZookeeperException {
        Preconditions.checkNotNull(subscription.getId());
        return new NewZkSubscriptionClient(
                subscription.getId(),
                zkHolder.getSubscriptionCurator(commitTimeoutMillis),
                objectMapper
        );
    }
}
