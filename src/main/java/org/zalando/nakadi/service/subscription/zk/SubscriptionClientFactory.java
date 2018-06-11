package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

@Service
public class SubscriptionClientFactory {
    private final ZooKeeperHolder zkHolder;
    private final ObjectMapper objectMapper;

    @Autowired
    public SubscriptionClientFactory(
            final ZooKeeperHolder zkHolder,
            final ObjectMapper objectMapper) {
        this.zkHolder = zkHolder;
        this.objectMapper = objectMapper;
    }

    public ZkSubscriptionClient createClient(final Subscription subscription, final String loggingPath)
            throws InternalNakadiException, NoSuchEventTypeException {
        Preconditions.checkNotNull(subscription.getId());
        return new NewZkSubscriptionClient(
                subscription.getId(),
                zkHolder.get(),
                loggingPath,
                objectMapper);
    }
}
