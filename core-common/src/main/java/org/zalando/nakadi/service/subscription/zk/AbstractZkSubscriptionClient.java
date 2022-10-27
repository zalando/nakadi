package org.zalando.nakadi.service.subscription.zk;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.OperationInterruptedException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

public abstract class AbstractZkSubscriptionClient implements ZkSubscriptionClient {
    private static final String STATE_INITIALIZED = "INITIALIZED";
    private static final int COMMIT_CONFLICT_RETRY_TIMES = 5;
    private static final int MAX_ZK_RESPONSE_SECONDS = 5;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractZkSubscriptionClient.class);
    protected static final String NODE_TOPOLOGY = "/topology";

    private final String subscriptionId;
    private final ZooKeeperHolder.CloseableCuratorFramework closeableCuratorFramework;
    private final String closeSubscriptionStream;

    public AbstractZkSubscriptionClient(
            final String subscriptionId,
            final ZooKeeperHolder.CloseableCuratorFramework closeableCuratorFramework) throws ZookeeperException {
        this.subscriptionId = subscriptionId;
        this.closeableCuratorFramework = closeableCuratorFramework;
        this.closeSubscriptionStream = getSubscriptionPath("/close_subscription_stream");
    }

    protected CuratorFramework getCurator() {
        return this.closeableCuratorFramework.getCuratorFramework();
    }

    protected ZooKeeperHolder.CloseableCuratorFramework getCloseableCuratorFramework() {
        return this.closeableCuratorFramework;
    }

    protected String getSubscriptionId() {
        return subscriptionId;
    }

    protected String getSubscriptionPath(final String value) {
        return "/nakadi/subscriptions/" + subscriptionId + value;
    }

    @Override
    public final void deleteSubscription() {
        try {
            getCurator().delete()
                    .deletingChildrenIfNeeded()
                    .forPath(getSubscriptionPath(""));
        } catch (final KeeperException.NoNodeException nne) {
            LOG.warn("Subscription to delete is not found in Zookeeper: {}", subscriptionId);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final boolean isSubscriptionCreatedAndInitialized() throws NakadiRuntimeException {
        // First step - check that state node was already written
        try {
            final String state = new String(getCurator().getData().forPath(getSubscriptionPath("/state")), UTF_8);
            return state.equals(STATE_INITIALIZED);
        } catch (final KeeperException.NoNodeException ex) {
            return false;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void fillEmptySubscription(final Collection<SubscriptionCursorWithoutToken> cursors) {
        try {
            createSessionsZNode();
            createOffsetZNodes(cursors);
            createTopologyZNode(cursors);
            createStateZNodeAsInitialized();
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    private void createSessionsZNode() throws Exception {
        LOG.info("Creating sessions root");
        try {
            getCurator().create()
                    .creatingParentsIfNeeded() // Important to create all nodes in hierarchy
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getSubscriptionPath("/sessions"));
        } catch (final KeeperException.NodeExistsException ex) {
            LOG.info("ZNode for {} exists, not creating new one", getSubscriptionPath("/sessions"));
        }
    }

    private void createOffsetZNodes(final Collection<SubscriptionCursorWithoutToken> cursors) throws Exception {
        LOG.info("Creating offsets");
        for (final SubscriptionCursorWithoutToken cursor : cursors) {
            try {
                getCurator().create().creatingParentsIfNeeded().forPath(
                        getOffsetPath(cursor.getEventTypePartition()),
                        cursor.getOffset().getBytes(UTF_8));
            } catch (final KeeperException.NodeExistsException ex) {
                LOG.info("Offset ZNode {}/{} exists, not creating a new one",
                        cursor.getEventType(), cursor.getPartition());
            }
        }
    }

    private void createStateZNodeAsInitialized() throws Exception {
        LOG.info("updating state");
        try {
            getCurator().create().forPath(getSubscriptionPath("/state"), STATE_INITIALIZED.getBytes(UTF_8));
        } catch (final KeeperException.NodeExistsException ex) {
            LOG.info("ZNode for {} exists, not creating new one", getSubscriptionPath("/state"));
        }
    }

    @Override
    public final void registerSession(final Session session) {
        LOG.info("Registering session " + session);
        try {
            final String clientPath = getSubscriptionPath("/sessions/" + session.getId());
            final byte[] sessionData = serializeSession(session);
            getCurator().create().withMode(CreateMode.EPHEMERAL).forPath(clientPath, sessionData);
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public final void unregisterSession(final Session session) {
        try {
            getCurator().delete().forPath(getSubscriptionPath("/sessions/" + session.getId()));
        } catch (final KeeperException.NoNodeException ke) {
            // It's OK that the session node was not there: all we want is to make sure it's deleted.
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    protected <K, V> Map<K, V> loadDataAsync(final Collection<K> keys,
                                             final Function<K, String> keyConverter,
                                             final BiFunction<K, byte[], V> valueConverter)
            throws ServiceTemporarilyUnavailableException, NakadiRuntimeException {
        final Map<K, V> result = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(keys.size());
        try {
            final MDCUtils.Context loggingContext = MDCUtils.getContext();
            for (final K key : keys) {
                final String zkKey = keyConverter.apply(key);
                getCurator().getData().inBackground((client, event) -> {
                    try (MDCUtils.CloseableNoEx ignore = MDCUtils.withContext(loggingContext)){
                        if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                            final V value = valueConverter.apply(key, event.getData());
                            synchronized (result) {
                                result.put(key, value);
                            }
                        } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                            LOG.warn("Unable to get {} data from zk. Node not found ", zkKey);
                        } else {
                            LOG.error(
                                    "Failed to get {} data from zk. status code: {}",
                                    zkKey, event.getResultCode());
                        }
                    } catch (RuntimeException ex) {
                        LOG.error("Failed to memorize {} key value", key, ex);
                    } finally {
                        latch.countDown();
                    }
                }).forPath(zkKey);
            }
        } catch (Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
        try {
            if (!latch.await(MAX_ZK_RESPONSE_SECONDS, TimeUnit.SECONDS)) {
                throw new ServiceTemporarilyUnavailableException("Failed to wait for zk response", null);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ServiceTemporarilyUnavailableException("Failed to wait for zk response", ex);
        }
        return result;
    }

    @Override
    public final Collection<Session> listSessions()
            throws SubscriptionNotInitializedException, NakadiRuntimeException, ServiceTemporarilyUnavailableException {
        for (int i = 0; i < 5; i++) {
            try {
                final List<String> sessions = getCurator().getChildren().forPath(getSubscriptionPath("/sessions"));
                final Map<String, Session> result = loadDataAsync(sessions,
                        key -> getSubscriptionPath("/sessions/" + key),
                        this::deserializeSession);
                if (result.size() == sessions.size()) {
                    return result.values();
                }
            } catch (final KeeperException.NoNodeException e) {
                throw new SubscriptionNotInitializedException(getSubscriptionId());
            } catch (Exception ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
        throw new ServiceTemporarilyUnavailableException("Failed to get all keys from ZK", null);
    }

    @Override
    public boolean isActiveSession(final String streamId) throws ServiceTemporarilyUnavailableException {
        try {
            return getCurator().checkExists().forPath(getSubscriptionPath("/sessions/" + streamId)) != null;
        } catch (final Exception ex) {
            throw new ServiceTemporarilyUnavailableException("Error communicating with zookeeper", ex);
        }
    }

    @Override
    public final boolean isCloseSubscriptionStreamsInProgress() {
        try {
            return getCurator().checkExists().forPath(closeSubscriptionStream) != null;
        } catch (final Exception e) {
            // nothing in the path
        }
        return false;
    }

    @Override
    public final Closeable subscribeForStreamClose(final Runnable listener)
            throws NakadiRuntimeException, UnsupportedOperationException {
        final NodeCache cursorResetCache = new NodeCache(getCurator(), closeSubscriptionStream);
        cursorResetCache.getListenable().addListener(listener::run);

        try {
            cursorResetCache.start();
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }

        return () -> {
            try {
                cursorResetCache.close();
            } catch (final IOException e) {
                throw new NakadiRuntimeException(e);
            }
        };
    }

    @Override
    public ZkSubscription<SubscriptionCursorWithoutToken> subscribeForOffsetChanges(
            final EventTypePartition key, final Runnable commitListener) {
        final String path = getOffsetPath(key);
        return new ZkSubscriptionImpl.ZkSubscriptionValueImpl<>(
                getCurator(),
                commitListener,
                data -> new SubscriptionCursorWithoutToken(
                        key.getEventType(), key.getPartition(), new String(data, UTF_8)),
                path);
    }

    @Override
    public final void closeSubscriptionStreams(final Runnable action, final long timeout)
            throws OperationTimeoutException, ZookeeperException, OperationInterruptedException,
            RequestInProgressException {
        ZkSubscription<List<String>> sessionsListener = null;
        boolean resetWasAlreadyInitiated = false;
        try {
            // close subscription connections
            getCurator().create().withMode(CreateMode.EPHEMERAL).forPath(closeSubscriptionStream);

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
                    if (sessionsListener.getData().isEmpty()) {
                        action.run();
                        return;
                    }
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
            LOG.error(e.getMessage(), e);
            throw new ZookeeperException("Unexpected problem occurred when resetting cursors", e);
        } finally {
            if (sessionsListener != null) {
                sessionsListener.close();
            }

            try {
                if (!resetWasAlreadyInitiated) {
                    getCurator().delete().forPath(closeSubscriptionStream);
                }
            } catch (final Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        throw new OperationTimeoutException("Timeout resetting cursors");
    }

    @Override
    public final ZkSubscription<List<String>> subscribeForSessionListChanges(final Runnable listener)
            throws NakadiRuntimeException {
        return new ZkSubscriptionImpl.ZkSubscriptionChildrenImpl(
                getCurator(), listener, getSubscriptionPath("/sessions"));
    }

    @Override
    public final Optional<ZkSubscriptionNode> getZkSubscriptionNode()
            throws SubscriptionNotInitializedException, NakadiRuntimeException {
        if (!isSubscriptionCreatedAndInitialized()) {
            return Optional.empty();
        }

        return Optional.of(new ZkSubscriptionNode(
                Arrays.asList(getTopology().getPartitions()),
                listSessions()));
    }

    public void forceCommitOffsets(final List<SubscriptionCursorWithoutToken> cursors) throws NakadiRuntimeException {
        try {
            for (final SubscriptionCursorWithoutToken cursor : cursors) {
                getCurator().setData().forPath(
                        getOffsetPath(cursor.getEventTypePartition()),
                        cursor.getOffset().getBytes(UTF_8));
            }
        } catch (Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @Override
    public List<Boolean> commitOffsets(
            final List<SubscriptionCursorWithoutToken> cursors) {
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
                            String newMaxOffset = currentMaxOffset;
                            final List<Boolean> commits = Lists.newArrayList();

                            for (final SubscriptionCursorWithoutToken cursor : entry.getValue()) {
                                // Offsets are lexicographically comparable, except 'BEGIN'
                                if (cursor.getOffset().compareTo(newMaxOffset) > 0
                                        || newMaxOffset.equalsIgnoreCase(Cursor.BEFORE_OLDEST_OFFSET)) {
                                    newMaxOffset = cursor.getOffset();
                                    commits.add(true);
                                } else {
                                    commits.add(false);
                                }
                            }
                            if (!newMaxOffset.equals(currentMaxOffset)) {
                                LOG.info("Committing {} to {}/{}",
                                        newMaxOffset, entry.getKey().getEventType(), entry.getKey().getPartition());

                                getCurator()
                                        .setData()
                                        .withVersion(stat.getVersion())
                                        .forPath(offsetPath, newMaxOffset.getBytes(Charsets.UTF_8));
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

    protected abstract void createTopologyZNode(Collection<SubscriptionCursorWithoutToken> cursors) throws Exception;

    protected abstract String getOffsetPath(EventTypePartition etp);

    protected abstract byte[] serializeSession(Session session) throws NakadiRuntimeException;

    protected abstract Session deserializeSession(String sessionId, byte[] sessionZkData) throws NakadiRuntimeException;

    @Override
    public void close() throws IOException {
        getCloseableCuratorFramework().close();
    }
}
