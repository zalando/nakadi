package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
     * Checks subscriptions streaming on this instance if there is rebalance required. If it is required - runs rebalance
     */
    @Scheduled(fixedRate = 100L)
    public void rebalanceSubscriptions() {
        eventStreams
                .stream()
                .map(EventStream::getSubscriptionId)
                .forEach(this::rebalanceSubscriptionIfNeeded);
    }

    private void rebalanceSubscriptionIfNeeded(final String subscriptionId) {
        subscriptionRepository
                .getTopology(subscriptionId)
                .ifPresent(topology -> {
                    if (currentTopologies.get(subscriptionId) == null ||
                            (!currentTopologies.get(subscriptionId).equals(topology))) {
                        applyNewTopology(subscriptionId, topology);
                    }
                });
    }

    /**
     * This method will change the partitions of streams running on this nakadi instance.
     * Should be run after partition rebalance happened
     *
     * @param subscriptionId the id of subscription for which partitions rebalance happened
     */
    public void applyNewTopology(final String subscriptionId, final Topology newTopology) {

        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);

        final List<Integer> clientsIndexes = eventStreams
                .stream()
                .filter(eventStream -> subscriptionId.equals(eventStream.getSubscriptionId()))
                .map((Function<EventStream, Integer>) eventStream -> newTopology
                        .getClientIndex(eventStream.getClientId())
                        .orElseThrow(() -> new NakadiRuntimeException("client is not part of topology")))
                .collect(Collectors.toList());

        final Map<Integer, List<TopicPartition>> newPartitionsForClients = getPartitionsForClients(subscriptionId,
                clientsIndexes,
                newTopology.getClientIds().size());

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
                            .map((Function<TopicPartition, Cursor>) tp -> subscription
                                    .getCursor(tp)
                                    .orElseThrow(() -> new NakadiRuntimeException("Cursor not found for topic/partition")))
                            .collect(Collectors.toList());

                    eventStream.changeDistribution(cursorsForClient);
                });

        currentTopologies.put(subscriptionId, newTopology);
    }

    private Map<Integer, List<TopicPartition>> getPartitionsForClients(final String subscriptionId,
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

    public void addEventStream(final EventStream eventStream) throws NakadiException {
        eventStreams.add(eventStream);

        final List<String> currentClientIds = subscriptionRepository
                .getTopology(eventStream.getSubscriptionId())
                .map(Topology::getClientIds)
                .orElse(Lists.newArrayList());

        final List<String> newClientIds = ImmutableList.<String>builder()
                .addAll(currentClientIds)
                .add(eventStream.getClientId())
                .build();

        final Topology newTopology = new Topology(newClientIds);
        subscriptionRepository.setNewTopology(eventStream.getSubscriptionId(), newTopology);

        rebalanceSubscriptionIfNeeded(eventStream.getSubscriptionId());
    }

    public void removeEventStream(final EventStream eventStream) throws NakadiException {
        eventStreams.remove(eventStream);

        final List<String> newClientIds = subscriptionRepository
                .getTopology(eventStream.getSubscriptionId())
                .map(Topology::getClientIds)
                .orElse(Lists.newArrayList())
                .stream()
                .filter(clientId -> !clientId.equals(eventStream.getClientId()))
                .collect(Collectors.toList());

        final Topology newTopology = new Topology(newClientIds);
        subscriptionRepository.setNewTopology(eventStream.getSubscriptionId(), newTopology);

        rebalanceSubscriptionIfNeeded(eventStream.getSubscriptionId());
    }

}


