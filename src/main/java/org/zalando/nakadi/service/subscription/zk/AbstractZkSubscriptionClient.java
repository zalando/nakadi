package org.zalando.nakadi.service.subscription.zk;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.exceptions.runtime.OperationInterruptedException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

public abstract class AbstractZkSubscriptionClient implements ZkSubscriptionClient {
    private static final String STATE_INITIALIZED = "INITIALIZED";
    private static final int COMMIT_CONFLICT_RETRY_TIMES = 5;
    protected static final String NODE_TOPOLOGY = "/topology";

    private final String subscriptionId;
    private final CuratorFramework curatorFramework;
    private InterProcessSemaphoreMutex lock;
    private final String resetCursorPath;
    private final Logger log;

    public AbstractZkSubscriptionClient(
            final String subscriptionId,
            final CuratorFramework curatorFramework,
            final String loggingPath) {
        this.subscriptionId = subscriptionId;
        this.curatorFramework = curatorFramework;
        this.resetCursorPath = getSubscriptionPath("/cursor_reset");
        this.log = LoggerFactory.getLogger(loggingPath + ".zk");
    }

    protected CuratorFramework getCurator() {
        return this.curatorFramework;
    }

    protected String getSubscriptionId() {
        return subscriptionId;
    }

    protected String getSubscriptionPath(final String value) {
        return "/nakadi/subscriptions/" + subscriptionId + value;
    }

    protected Logger getLog() {
        return log;
    }

    @Override
    public final void runLocked(final Runnable function) {
        try {
            Exception releaseException = null;
            if (null == lock) {
                lock = new InterProcessSemaphoreMutex(curatorFramework, "/nakadi/locks/subscription_" + subscriptionId);
            }

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
        } catch (final NakadiRuntimeException | MyNakadiRuntimeException1 e) {
            throw e;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void deleteSubscription() {
        try {
            final String subscriptionPath = getSubscriptionPath("");
            getCurator().delete().guaranteed().deletingChildrenIfNeeded().forPath(subscriptionPath);
        } catch (final KeeperException.NoNodeException nne) {
            getLog().warn("Subscription to delete is not found in Zookeeper: {}", subscriptionId);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final boolean isSubscriptionCreatedAndInitialized() throws NakadiRuntimeException {
        // First step - check that state node was already written
        try {
            final String state = new String(getCurator().getData().forPath(getSubscriptionPath("/state")), UTF_8);
            if (!state.equals(STATE_INITIALIZED)) {
                return false;
            }
            return true;
        } catch (final KeeperException.NoNodeException ex) {
            return false;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void fillEmptySubscription(final Collection<SubscriptionCursorWithoutToken> cursors) {
        try {
            // Delete root subscription node, if it was erroneously created
            if (null != getCurator().checkExists().forPath(getSubscriptionPath(""))) {
                deleteSubscription();
            }
            getLog().info("Creating sessions root");
            getCurator().create()
                    .creatingParentsIfNeeded() // Important to create all nodes in hierarchy
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getSubscriptionPath("/sessions"));

            final byte[] topologyData = createTopologyAndOffsets(cursors);
            getCurator().create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getSubscriptionPath(NODE_TOPOLOGY), topologyData);

            getLog().info("updating state");
            getCurator().create().forPath(getSubscriptionPath("/state"), STATE_INITIALIZED.getBytes(UTF_8));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void registerSession(final Session session) {
        getLog().info("Registering session " + session);
        try {
            final String clientPath = getSubscriptionPath("/sessions/" + session.getId());
            getCurator().create().withMode(CreateMode.EPHEMERAL).forPath(clientPath,
                    String.valueOf(session.getWeight()).getBytes(UTF_8));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void unregisterSession(final Session session) {
        try {
            getCurator().delete().guaranteed().forPath(getSubscriptionPath("/sessions/" + session.getId()));
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final Session[] listSessions() {
        getLog().info("fetching sessions information");
        final List<Session> sessions = new ArrayList<>();
        final List<String> zkSessions;
        try {
            zkSessions = getCurator().getChildren().forPath(getSubscriptionPath("/sessions"));
        } catch (final KeeperException.NoNodeException e) {
            throw new SubscriptionNotInitializedException(getSubscriptionId());
        } catch (Exception ex) {
            throw new NakadiRuntimeException(ex);
        }

        try {
            for (final String sessionId : zkSessions) {
                final int weight = Integer.parseInt(new String(getCurator().getData()
                        .forPath(getSubscriptionPath("/sessions/" + sessionId)), UTF_8));
                sessions.add(new Session(sessionId, weight));
            }
            return sessions.toArray(new Session[sessions.size()]);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public boolean isActiveSession(final String streamId) throws ServiceUnavailableException {
        try {
            return getCurator().checkExists().forPath(getSubscriptionPath("/sessions/" + streamId)) != null;
        } catch (final Exception ex) {
            throw new ServiceUnavailableException("Error communicating with zookeeper", ex);
        }
    }

    @Override
    public final boolean isCursorResetInProgress() {
        try {
            return getCurator().checkExists().forPath(resetCursorPath) != null;
        } catch (final Exception e) {
            // nothing in the path
        }
        return false;
    }

    @Override
    public final ZKSubscription subscribeForCursorsReset(final Runnable listener)
            throws NakadiRuntimeException, UnsupportedOperationException {
        final NodeCache cursorResetCache = new NodeCache(getCurator(), resetCursorPath);
        cursorResetCache.getListenable().addListener(listener::run);

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
    public ZKSubscription subscribeForOffsetChanges(final EventTypePartition key, final Runnable commitListener) {
        final String path = getOffsetPath(key);
        getLog().info("subscribeForOffsetChanges: {}, path: {}", key, path);
        return ChangeListener.forData(getCurator(), path, commitListener);
    }

    @Override
    public final void resetCursors(final List<SubscriptionCursorWithoutToken> cursors, final long timeout)
            throws OperationTimeoutException, ZookeeperException, OperationInterruptedException,
            RequestInProgressException {
        ZKSubscription sessionsListener = null;
        boolean resetWasAlreadyInitiated = false;
        try {
            // close subscription connections
            getCurator().create().withMode(CreateMode.EPHEMERAL).forPath(resetCursorPath);

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
                    if (getCurator().getChildren().forPath(getSubscriptionPath("/sessions")).isEmpty()) {
                        forceCommitOffsets(cursors);
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
            getLog().error(e.getMessage(), e);
            throw new ZookeeperException("Unexpected problem occurred when resetting cursors", e);
        } finally {
            if (sessionsListener != null) {
                sessionsListener.cancel();
            }

            try {
                if (!resetWasAlreadyInitiated) {
                    getCurator().delete().guaranteed().forPath(resetCursorPath);
                }
            } catch (final Exception e) {
                getLog().error(e.getMessage(), e);
            }
        }

        throw new OperationTimeoutException("Timeout resetting cursors");
    }

    @Override
    public final ZKSubscription subscribeForSessionListChanges(final Runnable listener) {
        getLog().info("subscribeForSessionListChanges: " + listener.hashCode());
        return ChangeListener.forChildren(getCurator(), getSubscriptionPath("/sessions"), listener);
    }

    @Override
    public final ZKSubscription subscribeForTopologyChanges(final Runnable onTopologyChanged) {
        getLog().info("subscribeForTopologyChanges");
        return ChangeListener.forData(getCurator(), getSubscriptionPath(NODE_TOPOLOGY), onTopologyChanged);
    }

    @Override
    public final ZkSubscriptionNode getZkSubscriptionNodeLocked() throws SubscriptionNotInitializedException {
        final ZkSubscriptionNode subscriptionNode = new ZkSubscriptionNode();
        try {
            if (null == getCurator().checkExists().forPath(getSubscriptionPath(""))) {
                return subscriptionNode;
            }
        } catch (final Exception e) {
            // Zk communication failure
            throw new NakadiRuntimeException(e);
        }

        try {
            runLocked(() -> {
                subscriptionNode.setPartitions(listPartitions());
                subscriptionNode.setSessions(listSessions());
            });
        } catch (final NakadiRuntimeException ex) {
            // this line intentionally left to have the same behavior as it was before
            getLog().info("No data about provided subscription {} in ZK", getSubscriptionPath(""));
        }

        return subscriptionNode;
    }

    private void forceCommitOffsets(final List<SubscriptionCursorWithoutToken> cursors) throws Exception {
        for (final SubscriptionCursorWithoutToken cursor : cursors) {
            getCurator().setData().forPath(
                    getOffsetPath(cursor.getEventTypePartition()),
                    cursor.getOffset().getBytes(UTF_8));
        }
    }

    @Override
    public List<Boolean> commitOffsets(
            final List<SubscriptionCursorWithoutToken> cursors,
            final Comparator<SubscriptionCursorWithoutToken> comparator) {
        final Map<EventTypePartition, List<SubscriptionCursorWithoutToken>> grouped =
                cursors.stream().collect(Collectors.groupingBy(SubscriptionCursorWithoutToken::getEventTypePartition));
        try {
            final Map<EventTypePartition, Iterator<Boolean>> committedOverall = new HashMap<>();
            for (final Map.Entry<EventTypePartition, List<SubscriptionCursorWithoutToken>> entry : grouped.entrySet()) {
                final String offsetPath = getOffsetPath(entry.getKey());
                final List<Boolean> committed;
                committed = executeWithRetry(() -> {
                            final Stat stat = new Stat();
                            final byte[] currentOffsetData = getCurator().getData().storingStatIn(stat)
                                    .forPath(offsetPath);
                            final String currentMaxOffset = new String(currentOffsetData, UTF_8);
                            SubscriptionCursorWithoutToken currentMaxCursor = new SubscriptionCursorWithoutToken(
                                    entry.getKey().getEventType(),
                                    entry.getKey().getPartition(),
                                    currentMaxOffset
                            );
                            final List<Boolean> commits = Lists.newArrayList();

                            for (final SubscriptionCursorWithoutToken cursor : entry.getValue()) {
                                if (comparator.compare(cursor, currentMaxCursor) > 0) {
                                    currentMaxCursor = cursor;
                                    commits.add(true);
                                } else {
                                    commits.add(false);
                                }
                            }
                            if (!currentMaxCursor.getOffset().equals(currentMaxOffset)) {
                                getLog().info("Committing {} to {}", currentMaxCursor.getOffset(), offsetPath);
                                getCurator()
                                        .setData()
                                        .withVersion(stat.getVersion())
                                        .forPath(offsetPath, currentMaxCursor.getOffset().getBytes(Charsets.UTF_8));
                            }
                            return commits;
                        },
                        new RetryForSpecifiedCountStrategy<List<Boolean>>(COMMIT_CONFLICT_RETRY_TIMES)
                                .withExceptionsThatForceRetry(KeeperException.BadVersionException.class));
                committedOverall.put(
                        entry.getKey(),
                        Optional.ofNullable(committed)
                                .orElse(Collections.nCopies(entry.getValue().size(), false))
                                .iterator());

            }
            return cursors.stream()
                    .map(cursor -> committedOverall.get(cursor.getEventTypePartition()).next())
                    .collect(Collectors.toList());

        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    protected abstract byte[] createTopologyAndOffsets(Collection<SubscriptionCursorWithoutToken> cursors)
            throws Exception;

    protected abstract String getOffsetPath(EventTypePartition etp);
}
