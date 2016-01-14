package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RegularPartitionDistributor implements PartitionDistributor{

    private final TopicRepository topicRepository;

    private final SubscriptionRepository subscriptionRepository;

    public RegularPartitionDistributor(final TopicRepository topicRepository,
                                       final SubscriptionRepository subscriptionRepository) {
        this.topicRepository = topicRepository;
        this.subscriptionRepository = subscriptionRepository;
    }

    @Override
    public Map<Integer, List<TopicPartition>> getPartitionsForClients(final String subscriptionId,
                                                                      final List<Integer> clientIndexes,
                                                                      final int clientsNum) {
        final List<TopicPartition> allPartitionsSorted = subscriptionRepository
                .getSubscription(subscriptionId)
                .getTopics()
                .stream()
                .flatMap(topic -> topicRepository
                        .listPartitions(topic)
                        .stream()
                        .map(partition -> new TopicPartition(topic, partition)))
                .sorted()
                .collect(Collectors.toList());

        return clientIndexes
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        clientIndex -> IntStream
                                .range(0, allPartitionsSorted.size())
                                .boxed()
                                .filter(tpIndex -> (tpIndex % clientsNum == clientIndex))
                                .map(allPartitionsSorted::get)
                                .collect(Collectors.toList())));
    }

}
