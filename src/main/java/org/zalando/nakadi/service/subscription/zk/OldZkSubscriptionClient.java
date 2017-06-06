package org.zalando.nakadi.service.subscription.zk;

import static com.google.common.base.Charsets.UTF_8;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

public class OldZkSubscriptionClient extends AbstractZkSubscriptionClient {
    private static final String NO_SESSION = "";

    public OldZkSubscriptionClient(final String subscriptionId, final CuratorFramework curatorFramework,
                                   final String loggingPath, final Map<String, String> topicToEventType) {
        super(subscriptionId, curatorFramework, loggingPath, topicToEventType);
    }

    /// Abstract part
    @Override
    protected boolean checkTopologyVersion(final byte[] bytes) throws ForwardVersionNotAllowed {
        //noinspection ResultOfMethodCallIgnored
        // It impossible to migrate to old version from new one, so ignore it
        try {
            Integer.parseInt(new String(bytes, UTF_8));
            return true;
        } catch (NumberFormatException ex) {
            throw new ForwardVersionNotAllowed();
        }
    }

    @Override
    protected byte[] createTopologyAndOffsets(final Collection<SubscriptionCursorWithoutToken> cursors)
            throws Exception {
        getLog().info("Creating topics");
        getCurator().create().withMode(CreateMode.PERSISTENT).forPath(getSubscriptionPath("/topics"));

        getLog().info("Creating partitions");
        for (final SubscriptionCursorWithoutToken cursor : cursors) {
            final String partitionPath = getPartitionPath(cursor.getEventTypePartition());
            getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(
                    partitionPath,
                    serializeNode(null, null, Partition.State.UNASSIGNED));
            getCurator().create().withMode(CreateMode.PERSISTENT).forPath(
                    getOffsetPath(cursor.getEventTypePartition()),
                    cursor.getOffset().getBytes(UTF_8));
        }
        getLog().info("creating topology node");
        return "0".getBytes(UTF_8);
    }

    private String guessTopic(final String eventType) {
        getLog().info("Guessing topic for event type {} from mapping {}",
                eventType,
                getTopicToEventType().entrySet().stream()
                        .map(ev -> ev.getKey() + ":" + ev.getValue())
                        .collect(Collectors.joining(", ")));
        return getTopicToEventType().entrySet().stream()
                .filter(kv -> kv.getValue().equalsIgnoreCase(eventType))
                .map(Map.Entry::getKey)
                .findAny().get();
    }

    // private part
    private String getPartitionPath(final EventTypePartition key) {
        return getSubscriptionPath("/topics/" + guessTopic(key.getEventType()) + "/" + key.getPartition());
    }

    private static byte[] serializeNode(@Nullable final String session, @Nullable final String nextSession,
                                        final Partition.State state) {
        return Stream.of(
                session == null ? NO_SESSION : session,
                nextSession == null ? NO_SESSION : nextSession,
                state.name()).collect(Collectors.joining(":")).getBytes(UTF_8);
    }

    public static Partition deserializeNode(
            final String eventType, final String partition, final byte[] data) {
        final String[] parts = new String(data, UTF_8).split(":");
        return new Partition(
                eventType, partition,
                NO_SESSION.equals(parts[0]) ? null : parts[0],
                NO_SESSION.equals(parts[1]) ? null : parts[1],
                Partition.State.valueOf(parts[2]));
    }

    @Override
    public void updatePartitionsConfiguration(final Partition[] partitions) throws NakadiRuntimeException {
        try {
            for (final Partition partition : partitions) {
                getLog().info("updating partition state: " + partition.getKey() + ":" + partition.getState() + ":"
                        + partition.getSession() + ":" + partition.getNextSession());
                getCurator().setData().forPath(
                        getPartitionPath(partition.getKey()),
                        serializeNode(partition.getSession(), partition.getNextSession(), partition.getState()));
            }
            incrementTopology();
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private void incrementTopology() {
        try {
            getLog().info("Incrementing topology version");
            final Integer curVersion = Integer.parseInt(new String(getCurator().getData()
                    .forPath(getSubscriptionPath(NODE_TOPOLOGY)), UTF_8));
            getCurator().setData().forPath(getSubscriptionPath(NODE_TOPOLOGY), String.valueOf(curVersion + 1)
                    .getBytes(UTF_8));
        } catch (NumberFormatException ex) {
            getLog().error("Probably using different topology type, trying to die");
            throw new NakadiRuntimeException(ex);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public Partition[] listPartitions() throws NakadiRuntimeException {
        getLog().info("fetching partitions information");

        final List<String> zkPartitions;
        try {
            zkPartitions = getCurator().getChildren().forPath(getSubscriptionPath("/topics"));
        } catch (KeeperException.NoNodeException ex) {
            throw new SubscriptionNotInitializedException(getSubscriptionId());
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
        try {
            final List<Partition> partitions = new ArrayList<>();
            for (final String topic : zkPartitions) {
                for (final String partition : getCurator().getChildren().forPath(getSubscriptionPath("/topics/"
                        + topic))) {
                    partitions.add(deserializeNode(
                            getTopicToEventType().get(topic),
                            partition,
                            getCurator().getData().forPath(getPartitionPath(
                                    new EventTypePartition(getTopicToEventType().get(topic), partition)))));
                }
            }
            return partitions.toArray(new Partition[partitions.size()]);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    protected String getOffsetPath(final EventTypePartition etp) {
        return getPartitionPath(etp) + "/offset";
    }

    @Override
    public SubscriptionCursorWithoutToken getOffset(final EventTypePartition key) {
        try {
            final String result =
                    new String(getCurator().getData().forPath(getOffsetPath(key)), UTF_8);
            if (Cursor.BEFORE_OLDEST_OFFSET.equalsIgnoreCase(result)) {
                return new SubscriptionCursorWithoutToken(key.getEventType(), key.getPartition(), result);
            }
            return new SubscriptionCursorWithoutToken(
                    key.getEventType(),
                    key.getPartition(),
                    KafkaCursor.toNakadiOffset(Long.parseLong(result)));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void transfer(final String sessionId, final Collection<EventTypePartition> partitions) {
        getLog().info("session " + sessionId + " releases partitions " + partitions);
        try {
            final List<Partition> changeSet = new ArrayList<>();
            for (final EventTypePartition pk : partitions) {
                final Partition realPartition = deserializeNode(
                        pk.getEventType(),
                        pk.getPartition(),
                        getCurator().getData().forPath(getPartitionPath(pk)));
                if (sessionId.equals(realPartition.getSession())
                        && realPartition.getState() == Partition.State.REASSIGNING) {
                    changeSet.add(
                            realPartition.toState(
                                    Partition.State.ASSIGNED,
                                    realPartition.getNextSession(),
                                    null));
                }
            }
            if (!changeSet.isEmpty()) {
                updatePartitionsConfiguration(changeSet.toArray(new Partition[changeSet.size()]));
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }
}
