package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Subscription client that uses topology as an object node and separates changeable data to a separate zk node.
 * The structure of zk data is like this:
 * <pre>
 * - nakadi
 * +- locks
 * | |- subscription_{subscription_id}      // Node that is being used to guarantee consistency of subscription data
 * |
 * +- subscriptions
 *   +- {subscription_id}
 *     |- state                             // Node for initialization finish tracking.
 *     |- cursor_reset                      // Optional node, presence of which says that cursor reset is in progress
 *     |                                    // During cursor reset process no new streams can be created. At the same
 *     |                                    // time all existing streaming sessions are being terminated.
 *     |- sessions                          // Node contains list of active sessions
 *     | |- {session_1}                     // Ephemeral node of session_1
 *     | |- ....
 *     | |- {session_N}                     // Ephemeral node of session_N
 *     |
 *     |- topology                          // Persistent node that holds all assignment information about partitions
 *     |                                    // Content is json serialized {@link Topology} object.
 *     |
 *     |- offsets                           // Node that holds up all the dynamic data for this subscription (offsets)
 *       |- {event_type_1}
 *       | |- 0                             // Nodes that contain view of offset (data from
 *       | |- ...                           // {@link SubscriptionCursorWithoutToken#getOffset()})
 *       | |- {event_type_1_max_partitions} // for specific (EventType, partition) pair.
 *       |
 *       |- ...
 *       |
 *       |- {event_type_M}
 *         |- 0
 *         |- ...
 *         |- {event_type_M_max_partitions}
 *
 *  </pre>
 */
public class NewZkSubscriptionClient extends AbstractZkSubscriptionClient {

    private final ObjectMapper objectMapper;
    private static final Logger LOG = LoggerFactory.getLogger(NewZkSubscriptionClient.class);

    public NewZkSubscriptionClient(
            final String subscriptionId,
            final ZooKeeperHolder.CloseableCuratorFramework closeableCuratorFramework,
            final ObjectMapper objectMapper) throws ZookeeperException {
        super(subscriptionId, closeableCuratorFramework);
        this.objectMapper = objectMapper;
    }

    @Override
    protected void createTopologyZNode(final Collection<SubscriptionCursorWithoutToken> cursors) throws Exception {
        final Partition[] partitions = cursors.stream().map(cursor -> new Partition(
                cursor.getEventType(),
                cursor.getPartition(),
                null,
                null,
                Partition.State.UNASSIGNED
        )).toArray(Partition[]::new);
        final Topology topology = new Topology(partitions, 0);
        LOG.info("Creating topology ZNode for {}", topology);
        final byte[] topologyData = objectMapper.writeValueAsBytes(topology);
        try {
            getCurator().create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getSubscriptionPath(NODE_TOPOLOGY), topologyData);
        } catch (final KeeperException.NodeExistsException ex) {
            LOG.info("ZNode exist for topology, not creating a new one");
        }
    }

    @Override
    public void updateTopology(final Function<Topology, Partition[]> partitioner)
            throws NakadiRuntimeException, SubscriptionNotInitializedException {
        Retryer.executeWithRetry(() -> {
                    final Stat stats = new Stat();
                    final Topology topology;
                    try {
                        topology = parseTopology(getCurator().getData()
                                .storingStatIn(stats)
                                .forPath(getSubscriptionPath(NODE_TOPOLOGY)));
                    } catch (KeeperException.NoNodeException ex) {
                        throw new SubscriptionNotInitializedException(getSubscriptionId());
                    } catch (final Exception ex) {
                        throw new NakadiRuntimeException(ex);
                    }

                    final Partition[] partitions = partitioner.apply(topology);
                    if (partitions.length > 0) {
                        final Topology newTopology =
                                topology.withUpdatedPartitions(partitions);
                        LOG.info("Updating topology to {}", newTopology);
                        try {
                            getCurator().setData().withVersion(stats.getVersion())
                                    .forPath(getSubscriptionPath(NODE_TOPOLOGY),
                                            objectMapper.writeValueAsBytes(newTopology));
                        } catch (final KeeperException.BadVersionException ex) {
                            throw ex;
                        } catch (final Exception ex) {
                            throw new NakadiRuntimeException(ex);
                        }
                    }

                    return null; // to use Callable
                },
                new RetryForSpecifiedCountStrategy()
                        .withMaxNumberOfRetries(10)
                        .withExceptionsThatForceRetry(KeeperException.BadVersionException.class));
    }

    @Override
    public Topology getTopology() throws NakadiRuntimeException,
            SubscriptionNotInitializedException {
        try {
            return parseTopology(getCurator().getData().forPath(getSubscriptionPath(NODE_TOPOLOGY)));
        } catch (KeeperException.NoNodeException ex) {
            throw new SubscriptionNotInitializedException(getSubscriptionId());
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    private Topology parseTopology(final byte[] data) {
        try {
            return objectMapper.readValue(data, Topology.class);
        } catch (IOException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final ZkSubscription<Topology> subscribeForTopologyChanges(final Runnable onTopologyChanged)
            throws NakadiRuntimeException {
        return new ZkSubscriptionImpl.ZkSubscriptionValueImpl<>(
                getCurator(),
                onTopologyChanged,
                this::parseTopology,
                getSubscriptionPath(NODE_TOPOLOGY));
    }

    protected byte[] serializeSession(final Session session)
            throws NakadiRuntimeException {
        try {
            return objectMapper.writeValueAsBytes(session);
        } catch (final JsonProcessingException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    protected Session deserializeSession(final String sessionId, final byte[] sessionZkData)
            throws NakadiRuntimeException {
        try {
            // old version of session: zkNode data is session weight
            final int weight = Integer.parseInt(new String(sessionZkData, UTF_8));
            return new Session(sessionId, weight, ImmutableList.of());
        } catch (final NumberFormatException nfe) {
            // new version of session: zkNode data is session object as json
            try {
                return objectMapper.readValue(sessionZkData, Session.class);
            } catch (final IOException e) {
                throw new NakadiRuntimeException(e);
            }
        }
    }

    protected String getOffsetPath(final EventTypePartition etp) {
        return getSubscriptionPath("/offsets/" + etp.getEventType() + "/" + etp.getPartition());
    }

    @Override
    public Map<EventTypePartition, SubscriptionCursorWithoutToken> getOffsets(
            final Collection<EventTypePartition> keys)
            throws NakadiRuntimeException, ServiceTemporarilyUnavailableException {
        final Map<EventTypePartition, SubscriptionCursorWithoutToken> offSets = loadDataAsync(keys,
                this::getOffsetPath, (etp, value) ->
                        new SubscriptionCursorWithoutToken(etp.getEventType(), etp.getPartition(),
                                new String(value, UTF_8)));

        if (offSets.size() != keys.size()) {
            throw new ServiceTemporarilyUnavailableException("Failed to get all the keys " +
                    keys.stream()
                            .filter(v -> !offSets.containsKey(v))
                            .map(String::valueOf)
                            .collect(Collectors.joining(", "))
                    + " from ZK.", null);
        }

        return offSets;
    }

    @Override
    public void transfer(final String sessionId, final Collection<EventTypePartition> partitions)
            throws NakadiRuntimeException, SubscriptionNotInitializedException {
        LOG.info("session " + sessionId + " releases partitions " + partitions);
        updateTopology(topology -> {
            final List<Partition> changeSet = new ArrayList<>();
            for (final EventTypePartition etp : partitions) {
                final Partition candidate = Stream.of(topology.getPartitions())
                        .filter(p -> p.getKey().equals(etp))
                        .findAny().get();
                if (sessionId.equals(candidate.getSession())
                        && candidate.getState() == Partition.State.REASSIGNING) {
                    changeSet.add(candidate.toState(
                            Partition.State.ASSIGNED,
                            candidate.getNextSession(),
                            null));
                }
            }

            return changeSet.toArray(new Partition[changeSet.size()]);
        });
    }

    @Override
    public void repartitionTopology(final String eventTypeName, final int newPartitionsCount, final String offset)
            throws NakadiRuntimeException {
        final Topology currentTopology = getTopology();
        final List<Partition> partitionsList = Lists.newArrayList(currentTopology.getPartitions());

        final int oldPartitionsCount = (int) partitionsList.stream().
                filter(partition -> partition.getEventType().equals(eventTypeName)).count();

        // Partitions can only be increased for an event type.
        // Add new partitions & create nodes for partitions' offset
        for (int index = oldPartitionsCount; index < newPartitionsCount; index++) {
            final String partition = String.valueOf(index);
            partitionsList.add(new Partition(
                    eventTypeName, partition, null, null, Partition.State.UNASSIGNED
            ));

            try {
                getCurator().create().creatingParentsIfNeeded().forPath(
                        getOffsetPath(new EventTypePartition(eventTypeName, partition)),
                        offset.getBytes(UTF_8));
            } catch (final Exception e) {
                throw new NakadiRuntimeException(e);
            }
        }

        final Topology partitionedTopology = new Topology(
                partitionsList.toArray(new Partition[0]),
                Optional.ofNullable(currentTopology.getVersion()).map(v -> v + 1).orElse(0)
        );

        try {
            LOG.info("Updating topology due to repartitioning event type: {} to {}", eventTypeName,
                    partitionedTopology);
            getCurator().setData().forPath(getSubscriptionPath(NODE_TOPOLOGY),
                    objectMapper.writeValueAsBytes(partitionedTopology));
        } catch (final Exception exception) {
            throw new NakadiRuntimeException(exception);
        }
    }
}
