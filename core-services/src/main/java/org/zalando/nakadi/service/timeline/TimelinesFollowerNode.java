package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.service.publishing.NamedThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The class is responsible for managing information about particular timelines node.
 * The class is acting as a blind follower to zookeeper notifications in regards to locked event types and proposed
 * configuration version.
 * <p>
 * In order to achieve this, this class is reading {@link TimelinesZookeeper#STATE_PATH} of type
 * {@link VersionedLockedEventTypes} that is used to notify about changes in locked event types and refreshes the
 * version that this class is compliant with in {@link TimelinesZookeeper#NODES_PATH}/{this_node_id}.
 */
@Component
@Profile("!test")
public class TimelinesFollowerNode {
    private final ScheduledExecutorService executorService;
    private final Logger log;
    private final TimelinesZookeeper timelinesZookeeper;
    private final LocalLockIntegration localLockIntegration;

    // The variable is accessible from one thread only, therefore no locking required.
    private VersionedLockedEventTypes currentVersion = VersionedLockedEventTypes.EMPTY;
    private Integer listenerCreationVersion = null;

    @Autowired
    public TimelinesFollowerNode(
            final TimelinesZookeeper timelinesZookeeper,
            final LocalLockIntegration localLockIntegration) {
        this.timelinesZookeeper = timelinesZookeeper;
        this.localLockIntegration = localLockIntegration;
        this.executorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("timelines-refresh-"));
        this.log = LoggerFactory.getLogger("timelines.node." + timelinesZookeeper.getNodeId());
    }

    @PostConstruct
    public void initializeNode() throws RuntimeException, JsonProcessingException, InterruptedException {
        log.info("Starting initialization");

        timelinesZookeeper.prepareZookeeperStructure();

        timelinesZookeeper.exposeSelfVersion(VersionedLockedEventTypes.EMPTY.getVersion());

        executorService.scheduleWithFixedDelay(this::refreshVersionSafe, 0, 1, TimeUnit.SECONDS);
        executorService.scheduleWithFixedDelay(this::periodicExposeSelfVersionSafe, 1, 1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void destroyNode() {
        log.info("Stopping node updates");
        executorService.shutdownNow();
    }

    /**
     * There may be a situation, when session was terminated, but we don't know about that/ignored this problem.
     * In order to avoid this problem, every now and then follower node exposes it's version (in order to ensure that
     * it's own node is still there).
     * Unfortunately, in case if there are many nodes running, it may create additional pressure on zookeeper. In order
     * to avoid this - exposure should not run too frequent (in case if zookeeper connection is not lost - this method
     * is not needed at all)
     */
    private void periodicExposeSelfVersionSafe() {
        try {
            log.debug("Exposing self version {} to ensure that node is still there", currentVersion.getVersion());
            timelinesZookeeper.exposeSelfVersion(currentVersion.getVersion());
        } catch (final RuntimeException ex) {
            log.warn("Failed to periodically expose self version", ex);
        } catch (final InterruptedException ex) {
            log.warn("Was interrupted while periodically exposing self version");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Periodically or by zookeeper event checks the version of timelines state in zookeeper. In case if it is changed
     * applies some actions. Most of the time (when timelines are not created) method acts in read-only mode, not
     * adding write pressure on zookeeper.
     */
    private void refreshVersionSafe() {
        try {
            log.debug("Running periodic refresh version");
            refreshVersion();
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

    private void refreshVersion() throws RuntimeException, InterruptedException {
        final TimelinesZookeeper.ZkVersionedLockedEventTypes zkData = timelinesZookeeper.getCurrentState(null);

        final boolean needCreateListener =
                null == this.listenerCreationVersion || // either listener was never created
                        !this.listenerCreationVersion.equals(zkData.zkVersion); // or change in node happened
        if (!Objects.equals(zkData.data.getVersion(), currentVersion.getVersion())) {
            log.info("Upgrading version from {} to {}", currentVersion.getVersion(), zkData.data.getVersion());
            // It must be safe to apply same version several times.
            log.info("Setting locked event types to {}", zkData.data.getLockedEts());
            localLockIntegration.setLockedEventTypes(zkData.data.getLockedEts());
            log.info("Exposing self version {}", zkData.data.getVersion());
            timelinesZookeeper.exposeSelfVersion(zkData.data.getVersion());
            currentVersion = zkData.data;
            log.info("Version upgrade finished");
        }

        if (needCreateListener) {
            log.info("Have to recreate listener. Previous zk version: {}, New zk version: {}",
                    listenerCreationVersion, zkData.zkVersion);
            final TimelinesZookeeper.ZkVersionedLockedEventTypes newZkData =
                    timelinesZookeeper.getCurrentState(this::zookeeperWatcherCalled);
            if (!Objects.equals(newZkData.zkVersion, zkData.zkVersion)) {
                // while setting up listener it was found that data has changed, we need to immediately reschedule
                // the check
                log.warn("While processing notification zookeeper state changed, will retrigger");
                this.executorService.submit(this::refreshVersionSafe);
            }
            this.listenerCreationVersion = newZkData.zkVersion;
        }
    }

    private void zookeeperWatcherCalled(final WatchedEvent event) {
        log.info("Received notification from zookeeper about change in active version: {}, {}",
                event.getType(), event.getState());
        switch (event.getType()) {
            case NodeDataChanged:
            case NodeCreated:
                executorService.submit(this::refreshVersionSafe);
                break;
            default:
                log.debug("Ignoring notification {} {}", event.getType(), event.getState());
        }
    }
}
