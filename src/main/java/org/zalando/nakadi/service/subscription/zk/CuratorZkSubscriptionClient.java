package org.zalando.nakadi.service.subscription.zk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import static com.google.common.base.Charsets.UTF_8;

public class CuratorZkSubscriptionClient implements ZkSubscriptionClient {
    private static final String STATE_INITIALIZED = "INITIALIZED";
    private static final String NO_SESSION = "";

    private final CuratorFramework curatorFramework;
    private final InterProcessSemaphoreMutex lock;
    private final String subscriptionId;
    private final Logger log;

    public CuratorZkSubscriptionClient(final String subscriptionId, final CuratorFramework curatorFramework) {
        this(subscriptionId, curatorFramework, CuratorZkSubscriptionClient.class.getName());
    }

    public CuratorZkSubscriptionClient(final String subscriptionId, final CuratorFramework curatorFramework,
                                       final String loggingPath) {
        this.subscriptionId = subscriptionId;
        this.curatorFramework = curatorFramework;
        this.lock = new InterProcessSemaphoreMutex(curatorFramework, "/nakadi/locks/subscription_" + subscriptionId);
        this.log = LoggerFactory.getLogger(loggingPath + ".zk");
    }

    @Override
    public void runLocked(final Runnable function) {
        try {
            Exception releaseException = null;

            lock.acquire();
            try {
                function.run();
            } finally {
                try {
                    lock.release();
                } catch (final Exception e) {
                    log.error("Failed to release lock", e);
                    releaseException = e;
                }
            }
            if (releaseException != null) {
                throw releaseException;
            }
        } catch (final NakadiRuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private String getSubscriptionPath(final String value) {
        return "/nakadi/subscriptions/" + subscriptionId + value;
    }

    private String getPartitionPath(final TopicPartition key) {
        return getSubscriptionPath("/topics/" + key.getTopic() + "/" + key.getPartition());
    }

    @Override
    public boolean isSubscriptionCreated() throws Exception {
        final Stat stat = curatorFramework.checkExists().forPath(getSubscriptionPath(""));
        return stat != null;
    }

    @Override
    public boolean createSubscription() {
        try {
            final String statePath = getSubscriptionPath("/state");
            final Stat stat = curatorFramework.checkExists().forPath(statePath);
            if (null == stat) {
                // node not exists.
                curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(statePath);
                return true;
            } else {
                final String state = new String(curatorFramework.getData().forPath(statePath), UTF_8);
                return !state.equals(STATE_INITIALIZED);
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void deleteSubscription() {
        try {
            final String subscriptionPath = getSubscriptionPath("");
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(subscriptionPath);
        } catch (final KeeperException.NoNodeException nne) {
            log.warn("Subscription to delete is not found in Zookeeper: {}", subscriptionId);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void fillEmptySubscription(final Collection<NakadiCursor> cursors) {
        try {
            log.info("Creating sessions root");
            if (null != curatorFramework.checkExists().forPath(getSubscriptionPath("/sessions"))) {
                curatorFramework.delete().guaranteed().deletingChildrenIfNeeded()
                        .forPath(getSubscriptionPath("/sessions"));
            }
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(getSubscriptionPath("/sessions"));

            log.info("Creating topics");
            if (null != curatorFramework.checkExists().forPath(getSubscriptionPath("/topics"))) {
                log.info("deleting topics recursively");
                curatorFramework.delete().guaranteed().deletingChildrenIfNeeded()
                        .forPath(getSubscriptionPath("/topics"));
            }
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(getSubscriptionPath("/topics"));

            log.info("Creating partitions");
            for (final NakadiCursor cursor : cursors) {
                final String partitionPath = getPartitionPath(cursor.getTopicPartition());
                curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(
                        partitionPath,
                        serializeNode(null, null, Partition.State.UNASSIGNED));
                curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(
                        partitionPath + "/offset",
                        cursor.getOffset().getBytes(UTF_8));
            }
            log.info("creating topology node");
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(getSubscriptionPath("/topology"),
                    "0".getBytes(UTF_8));
            log.info("updating state");
            curatorFramework.setData().forPath(getSubscriptionPath("/state"), STATE_INITIALIZED.getBytes(UTF_8));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private static byte[] serializeNode(@Nullable final String session, @Nullable final String nextSession,
                                        final Partition.State state) {
        return Stream.of(
                session == null ? NO_SESSION : session,
                nextSession == null ? NO_SESSION : nextSession,
                state.name()).collect(Collectors.joining(":")).getBytes(UTF_8);
    }

    public static Partition deserializeNode(final TopicPartition key, final byte[] data) {
        final String[] parts = new String(data, UTF_8).split(":");
        return new Partition(
                key,
                NO_SESSION.equals(parts[0]) ? null : parts[0],
                NO_SESSION.equals(parts[1]) ? null : parts[1],
                Partition.State.valueOf(parts[2]));
    }

    @Override
    public void updatePartitionConfiguration(final Partition partition) {
        try {
            log.info("updating partition state: " + partition.getKey() + ":" + partition.getState() + ":"
                    + partition.getSession() + ":" + partition.getNextSession());
            curatorFramework.setData().forPath(
                    getPartitionPath(partition.getKey()),
                    serializeNode(partition.getSession(), partition.getNextSession(), partition.getState()));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void incrementTopology() {
        try {
            log.info("Incrementing topology version");
            final Integer curVersion = Integer.parseInt(new String(curatorFramework.getData()
                    .forPath(getSubscriptionPath("/topology")), UTF_8));
            curatorFramework.setData().forPath(getSubscriptionPath("/topology"), String.valueOf(curVersion + 1)
                    .getBytes(UTF_8));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public Partition[] listPartitions() {
        log.info("fetching partitions information");
        try {
            final List<Partition> partitions = new ArrayList<>();
            for (final String topic : curatorFramework.getChildren().forPath(getSubscriptionPath("/topics"))) {
                for (final String partition : curatorFramework.getChildren().forPath(getSubscriptionPath("/topics/"
                        + topic))) {
                    final TopicPartition key = new TopicPartition(topic, partition);
                    partitions.add(deserializeNode(key, curatorFramework.getData().forPath(getPartitionPath(key))));
                }
            }
            return partitions.toArray(new Partition[partitions.size()]);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public Session[] listSessions() {
        log.info("fetching sessions information");
        final List<Session> sessions = new ArrayList<>();
        try {
            for (final String sessionId : curatorFramework.getChildren().forPath(getSubscriptionPath("/sessions"))) {
                final int weight = Integer.parseInt(new String(curatorFramework.getData()
                        .forPath(getSubscriptionPath("/sessions/" + sessionId)), UTF_8));
                sessions.add(new Session(sessionId, weight));
            }
            return sessions.toArray(new Session[sessions.size()]);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public ZKSubscription subscribeForSessionListChanges(final Runnable listener) {
        log.info("subscribeForSessionListChanges: " + listener.hashCode());
        return ChangeListener.forChildren(curatorFramework, getSubscriptionPath("/sessions"), listener);
    }

    @Override
    public ZKSubscription subscribeForTopologyChanges(final Runnable onTopologyChanged) {
        log.info("subscribeForTopologyChanges");
        return ChangeListener.forData(curatorFramework, getSubscriptionPath("/topology"), onTopologyChanged);
    }

    @Override
    public ZKSubscription subscribeForOffsetChanges(final TopicPartition key, final Runnable commitListener) {
        log.info("subscribeForOffsetChanges: {}", key);
        return ChangeListener.forData(curatorFramework, getPartitionPath(key) + "/offset", commitListener);
    }

    @Override
    public String getOffset(final TopicPartition key) {
        try {
            // TODO: Store nakadi cursor view in zk.
            final String result =
                    new String(curatorFramework.getData().forPath(getPartitionPath(key) + "/offset"), UTF_8);
            return KafkaCursor.toNakadiOffset(Long.parseLong(result));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void registerSession(final Session session) {
        log.info("Registering session " + session);
        try {
            final String clientPath = getSubscriptionPath("/sessions/" + session.getId());
            curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(clientPath,
                    String.valueOf(session.getWeight()).getBytes(UTF_8));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void unregisterSession(final Session session) {
        try {
            curatorFramework.delete().guaranteed().forPath(getSubscriptionPath("/sessions/" + session.getId()));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public void transfer(final String sessionId, final Collection<TopicPartition> partitions) {
        log.info("session " + sessionId + " releases partitions " + partitions);
        boolean changed = false;
        try {
            for (final TopicPartition pk : partitions) {
                final Partition realPartition = deserializeNode(
                        pk,
                        curatorFramework.getData().forPath(getPartitionPath(pk)));
                if (sessionId.equals(realPartition.getSession())
                        && realPartition.getState() == Partition.State.REASSIGNING) {
                    updatePartitionConfiguration(realPartition.toState(Partition.State.ASSIGNED,
                            realPartition.getNextSession(), null));
                    changed = true;
                }
            }
            if (changed) {
                incrementTopology();
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public ZkSubscriptionNode getZkSubscriptionNodeLocked() {
        final ZkSubscriptionNode subscriptionNode = new ZkSubscriptionNode();
        try {
            runLocked(() -> {
                subscriptionNode.setPartitions(listPartitions());
                subscriptionNode.setSessions(listSessions());
            });
        } catch (final NakadiRuntimeException nre) {
            final Exception cause = nre.getException();
            if (!(cause instanceof KeeperException.NoNodeException)) {
                throw new NakadiRuntimeException(cause);
            }
            log.info("No data about provided subscription {} in ZK", subscriptionId);
        }

        return subscriptionNode;
    }
}
