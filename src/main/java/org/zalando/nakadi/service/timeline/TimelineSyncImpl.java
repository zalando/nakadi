package org.zalando.nakadi.service.timeline;

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: Reinitialize on session recreation.
 */
@Service
public class TimelineSyncImpl {
    private static final String ROOT_PATH = "/timelines";
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final Logger LOG = LoggerFactory.getLogger(TimelineSyncImpl.class);

    private final ZooKeeperHolder zooKeeperHolder;
    private final InterProcessSemaphoreMutex lock;
    private final String thisId;
    private final Set<String> lockedEventTypes = new HashSet<>();
    private Integer cachedVersion;

    @Autowired
    public TimelineSyncImpl(ZooKeeperHolder zooKeeperHolder) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.thisId = UUID.randomUUID().toString();
        this.lock = new InterProcessSemaphoreMutex(zooKeeperHolder.get(), ROOT_PATH + "/lock");
        this.initializeZkStructure();
    }

    private String toZkPath(String path) {
        return ROOT_PATH + path;
    }

    private void initializeZkStructure() {
        runLocked(() -> {
            try {
                try {
                    // 1. Create version node, if needed, keep in mind that lock built root path
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/version"), "0".getBytes(CHARSET));

                    // 2. Create locked event types structure
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/locked_et"), "[]".getBytes(CHARSET));
                    // 3. Create nodes root path
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/nodes"), "[]".getBytes(CHARSET));
                } catch (KeeperException.NodeExistsException ignore) {
                }

                // 4. Get current version and locked event types
                refreshVersionUnlocked();
                // 5. Register self (if something already exists there - it's a problem.
                zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                        .forPath(toZkPath("/nodes/" + thisId), String.valueOf(cachedVersion).getBytes(CHARSET));

            } catch (Exception e) {
                LOG.error("Failed to initialize subscription api", e);
                throw new RuntimeException(e);
            }
        });
    }

    private void refreshVersionUnlocked() {
        try {
            final byte[] version = zooKeeperHolder.get()
                    .getData().usingWatcher((Watcher) this::versionChanged).forPath(toZkPath("/version"));
            this.cachedVersion = Integer.parseInt(
                    new String(version, CHARSET));
            final String etString = new String(
                    zooKeeperHolder.get().getData().forPath(toZkPath("/locked_et")), CHARSET);
            // TODO: Optimize
            this.lockedEventTypes.clear();
            this.lockedEventTypes.addAll(Stream.of(etString.split(",")).collect(Collectors.toSet()));
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void versionChanged(WatchedEvent we) {
        runLocked(this::refreshVersionUnlocked);
    }

    private void runLocked(Runnable action) {
        try {
            Exception releaseException = null;
            this.lock.acquire();
            try {
                action.run();
            } finally {
                try {
                    this.lock.release();
                } catch (Exception ex) {
                    releaseException = ex;
                }
            }
            if (null != releaseException) {
                throw releaseException;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
