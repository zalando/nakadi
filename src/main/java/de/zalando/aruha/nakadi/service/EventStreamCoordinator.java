package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EventStreamCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamCoordinator.class);

    private static final long SUBSCRIPTION_REBALANCE_CHECK_PERIOD_IN_MS = 100;

    private final SubscriptionRepository subscriptionRepository;

    private final PartitionDistributor partitionDistributor;

    private final TopicRepository topicRepository;

    private final List<EventStream> eventStreams;

    private final Map<String, Topology> currentTopologies;

    public EventStreamCoordinator(final SubscriptionRepository subscriptionRepository,
                                  final PartitionDistributor partitionDistributor,
                                  final TopicRepository topicRepository) {
        this.subscriptionRepository = subscriptionRepository;
        this.partitionDistributor = partitionDistributor;
        this.topicRepository = topicRepository;
        eventStreams = Collections.synchronizedList(Lists.newArrayList());
        currentTopologies = Collections.synchronizedMap(Maps.newHashMap()); // todo: we need somehow to lock it between reading and writing
    }

    /**
     * Creates new event stream and invokes clients rebalancing
     *
     * @param subscriptionId the id of subscription
     * @param outputStream the output stream where to stream data
     * @param config the configuration of stream
     * @return event stream created
     */
    public EventStream createEventStream(final String subscriptionId, final OutputStream outputStream,
                                         final EventStreamConfig config) {
        // add new client to topology and invoke rebalance
        final String newClientId = subscriptionRepository.generateNewClientId();
        subscriptionRepository.addClient(subscriptionId, newClientId);
        rebalanceSubscriptionIfNeeded(subscriptionId);

        // get partitions and cursors for this new client
        final Topology topology = subscriptionRepository.getTopology(subscriptionId);
        final int newClientIndex = topology.getClientIndex(newClientId).get();
        final int clientsNum = topology.getClientIds().size();

        final List<TopicPartition> partitionsForClient = partitionDistributor.getPartitionsForClients(subscriptionId,
                ImmutableList.of(newClientIndex), clientsNum).get(newClientIndex);

        final List<Cursor> cursors = partitionsForClient
                .stream()
                .map(tp -> subscriptionRepository.getCursor(subscriptionId, tp.getTopic(), tp.getPartition()))
                .collect(Collectors.toList());

        // create the event stream itself
        final EventConsumer eventConsumer = topicRepository.createEventConsumer(cursors);
        final EventStream eventStream = new EventStream(eventConsumer, outputStream, config, cursors);
        eventStream.setClientId(newClientId);
        eventStream.setSubscriptionId(subscriptionId);
        eventStreams.add(eventStream);
        return eventStream;
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
     * todo: in general this scheduled task should be probably replaced with watching appropriate zookeeper nodes
     */
    @Scheduled(fixedRate = SUBSCRIPTION_REBALANCE_CHECK_PERIOD_IN_MS)
    private void rebalanceNeededSubscriptions() {
        try {
            eventStreams
                    .stream()
                    .map(EventStream::getSubscriptionId)
                    .distinct()
                    .forEach(this::rebalanceSubscriptionIfNeeded);
        }
        catch (Exception e) {
            LOG.error("Error occurred during subscriptions rebalance", e);
        }
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


