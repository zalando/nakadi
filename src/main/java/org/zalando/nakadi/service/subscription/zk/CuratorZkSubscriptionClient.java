package org.zalando.nakadi.service.subscription.zk;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.OperationInterruptedException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CuratorZkSubscriptionClient implements ZkSubscriptionClient {
    private static final String STATE_INITIALIZED = "INITIALIZED";
    private static final String NO_SESSION = "";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final CuratorFramework curatorFramework;
    private final InterProcessSemaphoreMutex lock;
    private final String subscriptionId;
    private final Logger log;
    private final String resetCursorPath;

    public CuratorZkSubscriptionClient(final String subscriptionId, final CuratorFramework curatorFramework) {
        this(subscriptionId, curatorFramework, CuratorZkSubscriptionClient.class.getName());
    }

    public CuratorZkSubscriptionClient(final String subscriptionId, final CuratorFramework curatorFramework,
                                       final String loggingPath) {
        this.subscriptionId = subscriptionId;
        this.curatorFramework = curatorFramework;
        this.lock = new InterProcessSemaphoreMutex(curatorFramework, "/nakadi/locks/subscription_" + subscriptionId);
        this.log = LoggerFactory.getLogger(loggingPath + ".zk");
        this.resetCursorPath = getSubscriptionPath("/cursor_reset");
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

    private String getPartitionPath(final Partition.PartitionKey key) {
        return getSubscriptionPath("/topics/" + key.getTopic() + "/" + key.getPartition());
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
                final String state = new String(curatorFramework.getData().forPath(statePath), CHARSET);
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
    public void fillEmptySubscription(final Map<Partition.PartitionKey, String> partitionOffsets) {
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
            for (final Map.Entry<Partition.PartitionKey, String> p : partitionOffsets.entrySet()) {
                final String partitionPath = getPartitionPath(p.getKey());
                curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(
                        partitionPath,
                        serializeNode(null, null, Partition.State.UNASSIGNED));
                curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(
                        partitionPath + "/offset",
                        p.getValue().getBytes(CHARSET));
            }
            log.info("creating topology node");
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(getSubscriptionPath("/topology"),
                    "0".getBytes(CHARSET));
            log.info("updating state");
            curatorFramework.setData().forPath(getSubscriptionPath("/state"), STATE_INITIALIZED.getBytes(CHARSET));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private static byte[] serializeNode(@Nullable final String session, @Nullable final String nextSession,
                                        final Partition.State state) {
        return Stream.of(
                session == null ? NO_SESSION : session,
                nextSession == null ? NO_SESSION : nextSession,
                state.name()).collect(Collectors.joining(":")).getBytes(CHARSET);
    }

    public static Partition deserializeNode(final Partition.PartitionKey key, final byte[] data) {
        final String[] parts = new String(data, CHARSET).split(":");
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
                    .forPath(getSubscriptionPath("/topology")), CHARSET));
            curatorFramework.setData().forPath(getSubscriptionPath("/topology"), String.valueOf(curVersion + 1)
                    .getBytes(CHARSET));
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
                    final Partition.PartitionKey key = new Partition.PartitionKey(topic, partition);
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
                        .forPath(getSubscriptionPath("/sessions/" + sessionId)), CHARSET));
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
    public ZKSubscription subscribeForOffsetChanges(final Partition.PartitionKey key,
                                                    final Runnable commitListener) {
        log.info("subscribeForOffsetChanges");
        return ChangeListener.forData(curatorFramework, getPartitionPath(key) + "/offset", commitListener);
    }

    @Override
    public long getOffset(final Partition.PartitionKey key) {
        try {
            return Long.parseLong(new String(curatorFramework.getData().forPath(getPartitionPath(key) + "/offset"),
                    CHARSET));
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
                    String.valueOf(session.getWeight()).getBytes(CHARSET));
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
    public void transfer(final String sessionId, final Collection<Partition.PartitionKey> partitions) {
        log.info("session " + sessionId + " releases partitions " + partitions);
        boolean changed = false;
        try {
            for (final Partition.PartitionKey pk : partitions) {
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

    @Override
    public ZKSubscription subscribeForCursorsReset(final Runnable listener)
            throws NakadiRuntimeException, UnsupportedOperationException {
        final NodeCache cursorResetCache = new NodeCache(curatorFramework, resetCursorPath);
        cursorResetCache.getListenable().addListener(() -> listener.run());

        try {
            cursorResetCache.start();
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }

        return new ZKSubscription() {
            @Override
            public void refresh() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void cancel() {
                try {
                    cursorResetCache.getListenable().clear();
                    cursorResetCache.close();
                } catch (final IOException e) {
                    throw new NakadiRuntimeException(e);
                }
            }
        };
    }

    @Override
    public boolean isCursorResetInProgress() {
        try {
            return curatorFramework.checkExists().forPath(resetCursorPath) != null;
        } catch (final Exception e) {
            // nothing in the path
        }
        return false;
    }

    @Override
    public void resetCursors(final List<NakadiCursor> cursors, final long timeout) throws OperationTimeoutException,
            ZookeeperException, OperationInterruptedException, RequestInProgressException{
        ZKSubscription sessionsListener = null;
        boolean resetWasAlreadyInitiated = false;
        try {
            // close subscription connections
            curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(resetCursorPath);

            final AtomicBoolean sessionsChanged = new AtomicBoolean(true);
            sessionsListener = subscribeForSessionListChanges(() -> {
                sessionsChanged.set(true);
                synchronized (sessionsChanged) {
                    sessionsChanged.notifyAll();
                }
            });

            final long finishAt = System.currentTimeMillis() + timeout;
            while (finishAt > System.currentTimeMillis()) {
                if (sessionsChanged.compareAndSet(true, false)) {
                    if (curatorFramework.getChildren().forPath(getSubscriptionPath("/sessions")).isEmpty()) {
                        for (final NakadiCursor cursor : cursors) {
                            final String path = MessageFormat.format(getSubscriptionPath("/topics/{0}/{1}/offset"),
                                    cursor.getTopic(), cursor.getPartition());
                            curatorFramework.setData().forPath(path, cursor.getOffset().getBytes(Charsets.UTF_8));
                        }
                        return;
                    }
                    sessionsListener.refresh();
                }

                synchronized (sessionsChanged) {
                    sessionsChanged.wait(100);
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OperationInterruptedException("Resetting cursors is interrupted", e);
        } catch (final KeeperException.NodeExistsException e) {
            resetWasAlreadyInitiated = true;
            throw new RequestInProgressException("Cursors reset is already in progress for provided subscription", e);
        } catch (final KeeperException.NoNodeException e) {
            throw new UnableProcessException("Impossible to reset cursors for subscription", e);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            throw new ZookeeperException("Unexpected problem occurred when resetting cursors", e);
        } finally {
            if (sessionsListener != null) {
                sessionsListener.cancel();
            }

            try {
                if (!resetWasAlreadyInitiated) {
                    curatorFramework.delete().guaranteed().forPath(resetCursorPath);
                }
            } catch (final Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        throw new OperationTimeoutException("Timeout resetting cursors");
    }

}
