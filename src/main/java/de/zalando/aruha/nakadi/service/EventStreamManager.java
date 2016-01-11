package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ClientTopology;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.domain.TopicPartitionOffsets;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EventStreamManager {

    private final TopicRepository topicRepository;

    private final SubscriptionRepository subscriptionRepository;

    private final List<EventStream> eventStreams;

    private final Map<String, Topology> currentTopologies;

    public EventStreamManager(final TopicRepository topicRepository, final SubscriptionRepository subscriptionRepository) {
        this.topicRepository = topicRepository;
        this.subscriptionRepository = subscriptionRepository;
        eventStreams = Collections.synchronizedList(Lists.newArrayList());
        currentTopologies = Maps.newHashMap();
    }

    /**
     * This should be invoked when client new client starts / old client ends
     *
     * @param subscription the subscription for which partitions rebalance should be performed
     */
    public void rebalancePartitions(final Subscription subscription) throws NakadiException {
        final List<TopicPartitionOffsets> partitions = topicRepository.listPartitions(subscription.getTopic());
        final List<String> clientIds = subscription.getClientIds();

        final ImmutableMultimap.Builder<String, TopicPartition> builder = ImmutableMultimap.<String, TopicPartition>builder();
        IntStream
                .range(0, partitions.size())
                .boxed()
                .forEach(partitionIndex -> {
                    if (clientIds.size() > 0) {
                        builder.put(clientIds.get(partitionIndex % clientIds.size()),
                                new TopicPartition(subscription.getTopic(), partitions.get(partitionIndex).getPartitionId()));
                    }
                });
        final List<ClientTopology> clientTopologies = builder
                .build()
                .asMap()
                .entrySet()
                .stream()
                .map(entry -> new ClientTopology(entry.getKey(), Lists.newArrayList(entry.getValue()), false))
                .collect(Collectors.toList());

        final Long newVersion = subscriptionRepository.getNextTopologyVersion(subscription.getSubscriptionId());
        final Topology topology = new Topology(newVersion, clientTopologies);

        subscriptionRepository.addNewTopology(subscription.getSubscriptionId(), topology);
    }

    /**
     * Checks subscriptions streaming on this instance if there is rebalance required. If it is required - runs rebalance
     */
    @Scheduled(fixedRate = 100L)
    public void rebalanceWhereRequired() {
        eventStreams
                .stream()
                .map(EventStream::getSubscriptionId)
                .forEach(this::rebalanceForSubscriptionIfRequired);
    }

    private void rebalanceForSubscriptionIfRequired(final String subscriptionId) {
        subscriptionRepository
                .getTopologies(subscriptionId)
                .stream()
                .filter(topology ->
                        currentTopologies.get(subscriptionId) == null ||
                                currentTopologies.get(subscriptionId).getVersion() < topology.getVersion())
                .sorted((topology1, topology2) ->
                        topology1.getVersion().compareTo(topology2.getVersion()))
                .forEach(topology -> applyNewTopology(subscriptionId, topology));
    }

    /**
     * This method will change the partitions of streams running on this nakadi instance.
     * Should be run after partition rebalance happened
     *
     * @param subscriptionId the id of subscription for which partitions rebalance happened
     */
    public void applyNewTopology(final String subscriptionId, final Topology newTopology) {

        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final List<Cursor> subscriptionCursors = subscription.getCursors();

        final List<String> clientIdsRepartitioned = Lists.newArrayList();

        System.out.println("New topology");
        System.out.println(newTopology.getVersion());
        newTopology.getDistribution().stream().forEach(x -> {
            System.out.println(x.getClientId());
        });

        eventStreams
                .stream()
                .filter(eventStream -> subscriptionId.equals(eventStream.getSubscriptionId()))
                .forEach(eventStream -> {
                    final List<Cursor> clientCursors = newTopology
                            .getDistribution()
                            .stream()
                            .filter(clientTopology -> clientTopology.getClientId().equals(eventStream.getClientId()))
                            .findFirst()
                            .get()
                            .getPartitions()
                            .stream()
                            .map(tp ->
                                    subscriptionCursors
                                            .stream()
                                            .filter(c -> c.getTopic().equals(tp.getTopic())
                                                    && c.getPartition().equals(tp.getPartition()))
                                            .findFirst()
                                            .get())
                            .collect(Collectors.toList());

                    eventStream.changeDistribution(clientCursors);
                    clientIdsRepartitioned.add(eventStream.getClientId());
                });

        subscriptionRepository.clearProcessedRedistribution(subscriptionId, clientIdsRepartitioned);
        currentTopologies.put(subscriptionId, newTopology);
    }

    /*public void changeStreamsPartitionsAfterRebalance(final String subscriptionId,
                                                      final Multimap<String, String> newDistribution) {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
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

        subscriptionRepository.clearProcessedRedistribution(subscriptionId, clientIdsRepartitioned);
    }*/

    public void addEventStream(final EventStream eventStream) throws NakadiException {
        eventStreams.add(eventStream);
        final Subscription subscription = subscriptionRepository.getSubscription(eventStream.getSubscriptionId());
        rebalancePartitions(subscription);
        rebalanceForSubscriptionIfRequired(subscription.getSubscriptionId());
    }

    public void removeEventStream(final EventStream eventStream) throws NakadiException {
        eventStreams.remove(eventStream);
        final Subscription subscription = subscriptionRepository.getSubscription(eventStream.getSubscriptionId());
        rebalancePartitions(subscription);
    }

}


