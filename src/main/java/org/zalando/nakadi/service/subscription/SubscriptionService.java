package org.zalando.nakadi.service.subscription;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.ExceptionWrapper;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class SubscriptionService {

    private final EventTypeRepository eventTypeRepository;
    private final ZkSubscriptionClientFactory zkSubscriptionClientFactory;
    private final TopicRepository topicRepository;

    @Autowired
    public SubscriptionService(final ZkSubscriptionClientFactory zkSubscriptionClientFactory,
                               final TopicRepository topicRepository,
                               final EventTypeRepository eventTypeRepository) {
        this.zkSubscriptionClientFactory = zkSubscriptionClientFactory;
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public List<SubscriptionEventTypeStats> createSubscriptionStat(final Subscription subscription) {
        final ZkSubscriptionClient zkSubscriptionClient =
                zkSubscriptionClientFactory.createZkSubscriptionClient(subscription.getId());
        final Partition[] partitions = zkSubscriptionClient.listPartitions();
        return subscription.getEventTypes().stream()
                .map(ExceptionWrapper.wrapFunction(eventTypeRepository::findByNameO))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(eventType -> {
                    final Set<SubscriptionEventTypeStats.Partition> statPartitions = Arrays.stream(partitions)
                            .filter(partition -> eventType.getTopic().equals(partition.getKey().topic))
                            .map(ExceptionWrapper.wrapFunction(partition -> createPartition(zkSubscriptionClient, partition)))
                            .collect(Collectors.toSet());
                    return new SubscriptionEventTypeStats(eventType.getName(), statPartitions);
                })
                .collect(Collectors.toList());
    }

    private SubscriptionEventTypeStats.Partition createPartition(final ZkSubscriptionClient zkSubscriptionClient,
                                                                 final Partition partition) throws NakadiException {
        final String partitionName = partition.getKey().partition;
        final String partitionState = partition.getState().description;
        final String partitionSession = partition.getSession();
        final TopicPartition topicPartition = topicRepository.getPartition(partition.getKey().topic,
                partition.getKey().partition);
        final long clientOffset = zkSubscriptionClient.getOffset(partition.getKey());
        final long total = Long.valueOf(topicPartition.getNewestAvailableOffset());
        final long unconsumedEvents = total - clientOffset;
        return new SubscriptionEventTypeStats.Partition(
                partitionName, partitionState, unconsumedEvents, partitionSession);
    }

}
