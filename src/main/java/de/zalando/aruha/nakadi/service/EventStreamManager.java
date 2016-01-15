package de.zalando.aruha.nakadi.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EventStreamManager {

    private final SubscriptionRepository subscriptionRepository;

    private final PartitionDistributor partitionDistributor;

    private final List<EventStream> eventStreams;

    private final Map<String, Topology> currentTopologies;

    public EventStreamManager(final SubscriptionRepository subscriptionRepository,
                              final PartitionDistributor partitionDistributor) {
        this.subscriptionRepository = subscriptionRepository;
        this.partitionDistributor = partitionDistributor;
        eventStreams = Collections.synchronizedList(Lists.newArrayList());
        currentTopologies = Maps.newHashMap();
    }

    /**
     * Adds new stream and invokes rebalance of this stream subscription
     *
     * @param eventStream the stream to add
     */
    public void addEventStream(final EventStream eventStream) {
        eventStreams.add(eventStream);
        subscriptionRepository.addClient(eventStream.getSubscriptionId(), eventStream.getClientId());
        rebalanceSubscriptionIfNeeded(eventStream.getSubscriptionId());
    }

    /**
     * Removes finished stream and invokes rebalance of this stream subscription
     *
     * @param eventStream the stream to remove
     */
    public void removeEventStream(final EventStream eventStream) {
        eventStreams.remove(eventStream);
        subscriptionRepository.removeClient(eventStream.getSubscriptionId(), eventStream.getClientId());
        rebalanceSubscriptionIfNeeded(eventStream.getSubscriptionId());
    }

    /**
     * Checks if rebalance is needed for subscriptions running on this Nakadi instance
     */
    @Scheduled(fixedRate = 100L)
    private void rebalanceSubscriptions() {
        eventStreams
                .stream()
                .map(EventStream::getSubscriptionId)
                .distinct()
                .forEach(this::rebalanceSubscriptionIfNeeded);
    }

    /**
     * If new topology for this subscription found - rebalances according to new topology
     *
     * @param subscriptionId id of subscription to check for rebalance
     */
    private void rebalanceSubscriptionIfNeeded(final String subscriptionId) {
        final Topology topology = subscriptionRepository.getTopology(subscriptionId);
        if (currentTopologies.get(subscriptionId) == null || !currentTopologies.get(subscriptionId).equals(topology)) {
            applyNewTopology(subscriptionId, topology);
        }
    }

    /**
     * Changes the partitions of streams running on this nakadi instance for the subscription specified.
     *
     * @param subscriptionId the id of subscription for which partitions rebalance will be performed
     * @param newTopology the topology to apply
     */
    private void applyNewTopology(final String subscriptionId, final Topology newTopology) {

        // collect indexes of clients running on this Nakadi instance for this subscription
        final List<Integer> clientsIndexes = eventStreams
                .stream()
                .filter(eventStream -> subscriptionId.equals(eventStream.getSubscriptionId()))
                .map((Function<EventStream, Integer>) eventStream -> newTopology
                        .getClientIndex(eventStream.getClientId())
                        .orElseThrow(() -> new NakadiRuntimeException("client is not part of topology")))
                .collect(Collectors.toList());

        // calculate partitions for this clients
        final Map<Integer, List<TopicPartition>> newPartitionsForClients = partitionDistributor
                .getPartitionsForClients(subscriptionId, clientsIndexes, newTopology.getClientIds().size());

        // set new partitions distribution for streams running for this subscription on this Nakadi instance
        eventStreams
                .stream()
                .filter(eventStream -> subscriptionId.equals(eventStream.getSubscriptionId()))
                .forEach(eventStream -> {
                    final int clientIndex = newTopology
                            .getClientIndex(eventStream.getClientId())
                            .orElseThrow(() -> new NakadiRuntimeException("client is not part of topology"));

                    final List<Cursor> cursorsForClient = newPartitionsForClients
                            .get(clientIndex)
                            .stream()
                            .map(tp ->
                                    subscriptionRepository.getCursor(subscriptionId, tp.getTopic(), tp.getPartition()))
                            .collect(Collectors.toList());

                    eventStream.setOffsets(cursorsForClient);
                });

        // remember this new topology
        currentTopologies.put(subscriptionId, newTopology);
    }

}


