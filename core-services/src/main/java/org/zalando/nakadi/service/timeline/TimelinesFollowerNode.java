package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.publishing.NamedThreadFactory;
import org.zalando.nakadi.util.UUIDGenerator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The class is responsible for managing information about particular timelines node.
 * The class is acting as a blind follower to zookeeper notifications in regards to locked event types and proposed
 * configuration version.
 * <p>
 * In order to achieve this, this class is reading {@link TimelinesConfig#VERSION_PATH} of type
 * {@link VersionedLockedEventTypes} that is used to notify about changes in locked event types and refreshes the
 * version that this class is compliant with in {@link TimelinesConfig#NODES_PATH}/{this_node_id}.
 */
@Service
@Profile("!test")
public class TimelinesFollowerNode {
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("timelines-refresh"));

    private final String nodePath;
    private final Logger log;
    private final ZooKeeperHolder zookeeperHolder;
    private final LocalLockManager localLockManager;
    private final ObjectMapper objectMapper;

    // The variable is accessible from one thread only, therefore no locking required.
    private VersionedLockedEventTypes currentVersion = VersionedLockedEventTypes.EMPTY;
    private Integer listenerCreationVersion = null;

    @Autowired
    public TimelinesFollowerNode(
            final ZooKeeperHolder zookeeperHolder, final LocalLockManager localLockManager, final UUIDGenerator uuidGenerator, final ObjectMapper objectMapper) {
        final String nodeId = uuidGenerator.randomUUID().toString();
        this.zookeeperHolder = zookeeperHolder;
        this.localLockManager = localLockManager;
        this.nodePath = TimelinesConfig.getNodePath(nodeId);
        this.objectMapper = objectMapper;
        this.log = LoggerFactory.getLogger("timelines.node." + nodeId);
    }

    @PostConstruct
    public void initializeNode() throws RuntimeException, JsonProcessingException {
        log.info("Starting initialization");

        checkAndCreateZkNode(TimelinesConfig.VERSION_PATH,
                objectMapper.writeValueAsString(VersionedLockedEventTypes.EMPTY));
        checkAndCreateZkNode(TimelinesConfig.NODES_PATH, "");

        exposeSelfVersion(currentVersion);
        executorService.schedule(this::periodicRefreshVersionSafe, 1000, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroyNode() {
        log.info("Stopping node updates");
        executorService.shutdownNow();
    }

    private void checkAndCreateZkNode(final String fullPath, final String data) throws RuntimeException {
        try {
            zookeeperHolder.get().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(fullPath, data.getBytes(Charsets.UTF_8));
        } catch (final KeeperException.NodeExistsException ignore) {
            // skip it, node was already created
        } catch (final Exception e) {
            log.error("failed to create zookeeper node {}", fullPath, e);
            throw new RuntimeException(e);
        }
    }

    private void periodicRefreshVersionSafe() {
        try {
            periodicRefreshVersion();
        } catch (final RuntimeException ex) {
            // During the refresh something bad has happened. Usually that means that there are 2 possible problems:
            // 1. Zookeeper communication problem.
            // 2. Internal lock refresh problem.
            // In any case - it is required to execute the periodic refresh again, and the only question - after which
            // grace period it should be done.
            // Right now as there is no experience with behavior - it makes sense to rely on the default grace period
            // not to spam zookeeper with updates.
            log.error("Unexpected exception caught, can't throw it further because of single-threaded executor", ex);
        } catch (final InterruptedException ex) {
            log.warn("Interrupted waiting for changes, probably time to die", ex);
            Thread.currentThread().interrupt();
        }
    }

    private static <T> T unwrapInterruptedException(Callable<T> method) throws InterruptedException {
        try {
            return method.call();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            throw new RuntimeException(e);
        }
    }


    private void periodicRefreshVersion() throws RuntimeException, InterruptedException {
        final Stat stat = new Stat();

        final byte[] data = unwrapInterruptedException(
                () -> zookeeperHolder.get().getData()
                        .storingStatIn(stat)
                        .forPath(TimelinesConfig.VERSION_PATH));

        final boolean needCreateListener =
                null == this.listenerCreationVersion || // either listener was never created
                        !this.listenerCreationVersion.equals(stat.getVersion()); // or change in node happened

        final VersionedLockedEventTypes versionedData = VersionedLockedEventTypes.deserialize(objectMapper, data);

        if (!Objects.equals(versionedData.getVersion(), currentVersion.getVersion())) {
            // It must be safe to apply same version several times.
            localLockManager.setLockedEventTypes(versionedData.getLockedEts());
            exposeSelfVersion(versionedData);
            currentVersion = versionedData;
        }

        if (needCreateListener) {
            final Stat newStats = new Stat();
            unwrapInterruptedException(() -> zookeeperHolder.get().getData()
                    .storingStatIn(newStats)
                    .usingWatcher((Watcher) this::zookeeperWatcherCalled)
                    .forPath(TimelinesConfig.VERSION_PATH));
            if (newStats.getVersion() != stat.getVersion()) {
                // while setting up listener it was found that data has changed, we need to immediately reschedule
                // the check
                this.executorService.submit(this::periodicRefreshVersionSafe);
            }
            this.listenerCreationVersion = newStats.getVersion();
        }
    }

    private void zookeeperWatcherCalled(WatchedEvent event) {
        log.info("Received notification from zookeeper about change in active version: {}, {}",
                event.getType(), event.getState());
        switch (event.getType()) {
            case NodeDataChanged:
            case NodeCreated:
                executorService.submit(this::periodicRefreshVersionSafe);
        }
    }

    private void exposeSelfVersion(final VersionedLockedEventTypes data) throws RuntimeException {
        final byte[] versionBytes = String.valueOf(data.getVersion()).getBytes(Charsets.UTF_8);
        try {
            try {
                zookeeperHolder.get().setData().forPath(nodePath, versionBytes);
            } catch (final KeeperException.NoNodeException ex) {
                zookeeperHolder.get().create().withMode(CreateMode.EPHEMERAL).forPath(nodePath, versionBytes);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to expose self version for timelines management", ex);
        }
    }
}
