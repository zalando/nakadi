package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.utils.MultimapCollector;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.function.Function.identity;

public class EventStreamManager {

    private final TopicRepository topicRepository;

    private final SubscriptionRepository subscriptionRepository;

    private final List<EventStream> eventStreams;

    public EventStreamManager(final TopicRepository topicRepository, final SubscriptionRepository subscriptionRepository) {
        this.topicRepository = topicRepository;
        this.subscriptionRepository = subscriptionRepository;
        eventStreams = Collections.synchronizedList(Lists.newArrayList());
    }

    /**
     * This should be invoked when client new client starts / old client ends
     *
     * @param subscription the subscription for which partitions rebalance should be performed
     */
    public void rebalancePartitions(final Subscription subscription) throws NakadiException {
        final List<TopicPartition> partitions = topicRepository.listPartitions(subscription.getTopic());
        final List<String> clientIds = subscription.getClientIds();

        // client1 -> p0, p2, p4 ...
        // client2 -> p1, p3, p5 ...
        final Multimap<String, String> newPartitionDistribution = IntStream
                .range(0, partitions.size())
                .boxed()
                .collect(MultimapCollector.toMultimap(
                        partitionIndex -> clientIds.get(partitionIndex % clientIds.size()),
                        index -> partitions.get(index).getPartitionId()));

        // todo: write new partitioning to distributed storage
    }

    @Scheduled(fixedRate = 100L)
    public void checkIfRebalanceRequired() {

    }

    /**
     * This method will change the partitions of streams running on this nakadi instance.
     * Should be run after partition rebalance happened
     *
     * @param subscriptionId the id of subscription for which partitions rebalance happened
     */
    public void changeStreamsPartitionsAfterRebalance(final String subscriptionId) {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final Multimap<String, String> newDistribution = ArrayListMultimap.create(); // todo: get this from distibuted storage
        final Map<String, String> committedOffsets = subscription.getCursors();

        final List<String> clientIdsRepartitioned = Lists.newArrayList();

        eventStreams
                .stream()
                .filter(eventStream -> subscriptionId.equals(eventStream.getSubscriptionId()))
                .forEach(eventStream -> {
                    final Map<String, String> distributionWithOffsets = newDistribution.get(eventStream.getClientId())
                            .stream()
                            .collect(Collectors.toMap(
                                    identity(),
                                    committedOffsets::get
                            ));
                    eventStream.changeDistribution(distributionWithOffsets);
                    clientIdsRepartitioned.add(eventStream.getClientId());
                });

        // todo: cliendIdsRepartitioned: remove from distributed storage the rebalance info for clientIds
        // todo: for which we now changed the partitions to stream from
    }

    public void addEventStream(final EventStream eventStream) {
        eventStreams.add(eventStream);
    }

    public void removeEventStream(final EventStream eventStream) {
        eventStreams.remove(eventStream);
    }

}


