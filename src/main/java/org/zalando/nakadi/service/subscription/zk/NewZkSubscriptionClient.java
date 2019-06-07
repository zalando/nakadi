package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.zookeeper.KeeperException;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    public NewZkSubscriptionClient(
            final String subscriptionId,
            final ZooKeeperHolder zooKeeperHolder,
            final String loggingPath,
            final ObjectMapper objectMapper) {
        super(subscriptionId, zooKeeperHolder, loggingPath);
        this.objectMapper = objectMapper;
    }

    @Override
    protected byte[] createTopologyAndOffsets(final Collection<SubscriptionCursorWithoutToken> cursors)
            throws Exception {
        for (final SubscriptionCursorWithoutToken cursor : cursors) {
            getCurator().create().creatingParentsIfNeeded().forPath(
                    getOffsetPath(cursor.getEventTypePartition()),
                    cursor.getOffset().getBytes(UTF_8));
        }
        final Partition[] partitions = cursors.stream().map(cursor -> new Partition(
                cursor.getEventType(),
                cursor.getPartition(),
                null,
                null,
                Partition.State.UNASSIGNED
        )).toArray(Partition[]::new);
        final Topology topology = new Topology(partitions, "", 0);
        getLog().info("Generating topology {}", topology);
        return objectMapper.writeValueAsBytes(topology);
    }

    @Override
    public void updatePartitionsConfiguration(
            final String newSessionsHash, final Partition[] partitions) throws NakadiRuntimeException,
            SubscriptionNotInitializedException {
        final Topology newTopology = getTopology().withUpdatedPartitions(newSessionsHash, partitions);
        try {
            getLog().info("Updating topology to {}", newTopology);
            getCurator().setData().forPath(
                    getSubscriptionPath(NODE_TOPOLOGY),
                    objectMapper.writeValueAsBytes(newTopology));
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
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
        getLog().info("subscribeForTopologyChanges");
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
        getLog().info("session " + sessionId + " releases partitions " + partitions);
        final Topology topology = getTopology();

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
        if (!changeSet.isEmpty()) {
            // The list of sessions didn't change, therefore one should not update sessionsHash.
            updatePartitionsConfiguration(
                    topology.getSessionsHash(),
                    changeSet.toArray(new Partition[changeSet.size()]));
        }
    }

}
