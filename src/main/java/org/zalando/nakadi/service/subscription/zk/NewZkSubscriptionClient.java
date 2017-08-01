package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    public static class Topology {
        private final Partition[] partitions;
        private final int version;

        public Topology(
                @JsonProperty("partitions") final Partition[] partitions,
                @JsonProperty("version") final int version) {
            this.partitions = partitions;
            this.version = version;
        }

        public Partition[] getPartitions() {
            return partitions;
        }

        public Topology withUpdatedPartitions(final Partition[] partitions) {
            final Partition[] resultPartitions = Arrays.copyOf(this.partitions, this.partitions.length);
            for (final Partition newValue : partitions) {
                int selectedIdx = -1;
                for (int idx = 0; idx < resultPartitions.length; ++idx) {
                    if (resultPartitions[idx].getKey().equals(newValue.getKey())) {
                        selectedIdx = idx;
                    }
                }
                if (selectedIdx < 0) {
                    throw new MyNakadiRuntimeException1(
                            "Failed to find partition " + newValue.getKey() + " in " + this);
                }
                resultPartitions[selectedIdx] = newValue;
            }
            return new Topology(resultPartitions, version + 1);
        }

        @Override
        public String toString() {
            return "Topology{" +
                    "partitions=" + Arrays.toString(partitions) +
                    ", version=" + version +
                    '}';
        }
    }

    public NewZkSubscriptionClient(
            final String subscriptionId,
            final CuratorFramework curatorFramework,
            final String loggingPath,
            final ObjectMapper objectMapper) {
        super(subscriptionId, curatorFramework, loggingPath);
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
        final Topology topology = new Topology(partitions, 0);
        return objectMapper.writeValueAsBytes(topology);
    }

    @Override
    public void updatePartitionsConfiguration(final Partition[] partitions) throws NakadiRuntimeException,
            SubscriptionNotInitializedException {
        final Topology newTopology = readTopology().withUpdatedPartitions(partitions);
        try {
            getCurator().setData().forPath(
                    getSubscriptionPath(NODE_TOPOLOGY),
                    objectMapper.writeValueAsBytes(newTopology));
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    private Topology readTopology() throws NakadiRuntimeException,
            SubscriptionNotInitializedException {
        try {
            final byte[] data = getCurator().getData().forPath(getSubscriptionPath(NODE_TOPOLOGY));
            final Topology result = objectMapper.readValue(data, Topology.class);
            getLog().info("Topology is {}", result);
            return result;
        } catch (KeeperException.NoNodeException ex) {
            throw new SubscriptionNotInitializedException(getSubscriptionId());
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    @Override
    public Partition[] listPartitions() throws NakadiRuntimeException, SubscriptionNotInitializedException {
        return readTopology().getPartitions();
    }

    protected String getOffsetPath(final EventTypePartition etp) {
        return getSubscriptionPath("/offsets/" + etp.getEventType() + "/" + etp.getPartition());
    }

    @Override
    public SubscriptionCursorWithoutToken getOffset(final EventTypePartition key) throws NakadiRuntimeException {
        try {
            final String offset = new String(getCurator().getData().forPath(getOffsetPath(key)), UTF_8);
            return new SubscriptionCursorWithoutToken(key.getEventType(), key.getPartition(), offset);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void transfer(final String sessionId, final Collection<EventTypePartition> partitions)
            throws NakadiRuntimeException, SubscriptionNotInitializedException {
        getLog().info("session " + sessionId + " releases partitions " + partitions);
        final Topology topology = readTopology();

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
            updatePartitionsConfiguration(changeSet.toArray(new Partition[changeSet.size()]));
        }
    }

}
