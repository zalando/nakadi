package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;

@Service
public class SubscriptionClientFactory {
    private final ZooKeeperHolder zkHolder;
    private final FeatureToggleService featureToggleService;
    private final ObjectMapper objectMapper;
    private final TimelineService timelineService;

    @Autowired
    public SubscriptionClientFactory(
            final ZooKeeperHolder zkHolder,
            final FeatureToggleService featureToggleService,
            final ObjectMapper objectMapper,
            final TimelineService timelineService) {
        this.zkHolder = zkHolder;
        this.featureToggleService = featureToggleService;
        this.objectMapper = objectMapper;
        this.timelineService = timelineService;
    }

    public ZkSubscriptionClient createClient(final Subscription subscription, final String loggingPath)
            throws InternalNakadiException, NoSuchEventTypeException {
        Preconditions.checkNotNull(subscription.getId());
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.HILA_USE_TOPOLOGY_OBJECT)) {
            return new NewZkSubscriptionClient(
                    subscription.getId(),
                    zkHolder.get(),
                    loggingPath,
                    objectMapper,
                    timelineService.createTopicToEventTypeMapper(subscription.getEventTypes()));
        }
        return new OldZkSubscriptionClient(
                subscription.getId(), zkHolder.get(), loggingPath,
                timelineService.createTopicToEventTypeMapper(subscription.getEventTypes()));
    }
}
