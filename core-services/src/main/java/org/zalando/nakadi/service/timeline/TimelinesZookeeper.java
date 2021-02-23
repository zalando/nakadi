package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.util.UUIDGenerator;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * The purpose of this class is to hide zookeeper specific code related to timelines and not to mess up with timelines
 * logic. The class Is pretty straightforward in actions it does and doesn't require tests (apart from acceptance tests
 * that actually check that timelines are working).
 */
@Service
public class TimelinesZookeeper {
    private static final String ROOT_PATH = "/nakadi/timelines";
    private static final String VERSION_PATH = ROOT_PATH + "/state";
    private static final String NODES_PATH = ROOT_PATH + "/nodes";

    private final ZooKeeperHolder zkHolder;
    private final UUID nodeId;
    private final ObjectMapper objectMapper;

    public static class ZkVersionedLockedEventTypes {
        public final VersionedLockedEventTypes data;
        public final Integer zkVersion;

        public ZkVersionedLockedEventTypes(final VersionedLockedEventTypes data, final Integer zkVersion) {
            this.data = data;
            this.zkVersion = zkVersion;
        }
    }

    @Autowired
    public TimelinesZookeeper(
            final ZooKeeperHolder zkHolder, final UUIDGenerator uuidGenerator, final ObjectMapper objectMapper) {
        this.zkHolder = zkHolder;
        this.nodeId = uuidGenerator.randomUUID();
        this.objectMapper = objectMapper;
    }

    /**
     * Returns this node id. Exposed mostly for logging purposes.
     *
     * @return UUID of this node.
     */
    public UUID getNodeId() {
        return this.nodeId;
    }

    public void prepareZookeeperStructure() throws JsonProcessingException, InterruptedException {
        checkAndCreateZkNode(VERSION_PATH, objectMapper.writeValueAsBytes(new VersionedLockedEventTypes(
                1000L, Collections.emptySet()
        )));
        checkAndCreateZkNode(NODES_PATH, new byte[]{});
    }

    public ZkVersionedLockedEventTypes getCurrentVersion(@Nullable final Watcher watcher) throws InterruptedException {
        final Stat stat = new Stat();
        final byte[] data = unwrapInterruptedException(
                () -> {
                    final WatchPathable<byte[]> tmp = zkHolder.get().getData().storingStatIn(stat);
                    if (null != watcher) {
                        return tmp.usingWatcher(watcher).forPath(VERSION_PATH);
                    } else {
                        return tmp.forPath(VERSION_PATH);
                    }
                }
        );

        return new ZkVersionedLockedEventTypes(
                VersionedLockedEventTypes.deserialize(objectMapper, data),
                stat.getVersion());
    }

    public boolean setCurrentVersion(final VersionedLockedEventTypes data, final Integer oldVersion)
            throws InterruptedException {
        final byte[] bytes = data.serialize(objectMapper);

        return unwrapInterruptedException(() -> {
            try {
                zkHolder.get()
                        .setData()
                        .withVersion(oldVersion)
                        .forPath(VERSION_PATH, bytes);
                return true;
            } catch (final KeeperException.BadVersionException ex) {
                return false;
            }
        });
    }

    public Map<String, Long> getNodesVersions() throws InterruptedException {
        return unwrapInterruptedException(() -> {
            final Map<String, Long> result = new HashMap<>();
            final List<String> nodes = zkHolder.get().getChildren().forPath(NODES_PATH);
            for (final String node : nodes) {
                final Long nodeVersion = Long.parseLong(
                        new String(
                                zkHolder.get().getData().forPath(NODES_PATH + "/" + node),
                                StandardCharsets.UTF_8));
                result.put(node, nodeVersion);
            }
            return result;
        });
    }

    public void exposeSelfVersion(final Long version) throws RuntimeException, InterruptedException {
        final byte[] versionBytes = String.valueOf(version).getBytes(StandardCharsets.UTF_8);
        unwrapInterruptedException(() -> {
            final String nodePath = NODES_PATH + "/" + nodeId;
            try {
                zkHolder.get().setData().forPath(nodePath, versionBytes);
            } catch (final KeeperException.NoNodeException ex) {
                zkHolder.get().create().withMode(CreateMode.EPHEMERAL).forPath(nodePath, versionBytes);
            }
            return null;
        });
    }

    private void checkAndCreateZkNode(final String fullPath, final byte[] data)
            throws RuntimeException, InterruptedException {
        try {
            zkHolder.get().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(fullPath, data);
        } catch (final KeeperException.NodeExistsException ignore) {
            // skip it, node was already created
        } catch (final InterruptedException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T unwrapInterruptedException(final Callable<T> method) throws InterruptedException {
        try {
            return method.call();
        } catch (final InterruptedException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
