package org.zalando.nakadi.service;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;

import java.util.List;
import java.util.UUID;

import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;
import static org.zalando.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;

@Service
public class ConsumerLimitingService {

    private static final Logger LOG = LoggerFactory.getLogger(CursorsService.class);

    private final ZooKeeperHolder zkHolder;
    private final int maxConnections;
    private final ZooKeeperLockFactory zkLockFactory;

    @Autowired
    public ConsumerLimitingService(final ZooKeeperHolder zkHolder,
                                   final ZooKeeperLockFactory zkLockFactory,
                                   @Value("${nakadi.stream.maxConnections}") final int maxConnections) {
        this.zkHolder = zkHolder;
        this.zkLockFactory = zkLockFactory;
        this.maxConnections = maxConnections;
    }

    public List<ConnectionSlot> acquireConnectionSlots(final String client, final String eventType,
                                                       final List<String> partitions)
            throws NoConnectionSlotsException, ServiceUnavailableException {
        try {
            final String lockPath = format("/nakadi/consumers/locks/{0}|{1}", client, eventType);
            final InterProcessLock lock = zkLockFactory.createLock(lockPath);

            return runLocked(() -> {
                final List<String> notAllowed = partitions.stream()
                        .filter(partition -> !connectionAllowed(client, eventType, partition))
                        .collect(toList());
                if (!notAllowed.isEmpty()) {
                    final String msg = format("You exceeded the maximum number of simultaneous connections to a " +
                            "single partition for event type '{0}', partition(s): {1}; max limit is {2} connections " +
                            "per client", eventType, String.join(", ", notAllowed), maxConnections);
                    throw new NoConnectionSlotsException(msg);
                }

                @SuppressWarnings("UnnecessaryLocalVariable")
                final List<ConnectionSlot> connectionIds = partitions.stream()
                        .map(partition -> {
                            final String connectionId = acquireConnection(client, eventType, partition);
                            return new ConnectionSlot(client, eventType, partition, connectionId);
                        })
                        .collect(toList());
                return connectionIds;
            }, lock);

        } catch (final NoConnectionSlotsException | ServiceUnavailableException e) {
            throw e;
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Error communicating with zookeeper", e);
        }
    }

    public void releaseConnectionSlots(final List<ConnectionSlot> connectionSlots) {
        connectionSlots.forEach(this::releaseConnection);
    }

    private void releaseConnection(final ConnectionSlot connectionSlot) {
        final String parent = zkPathForConsumer(connectionSlot.getClient(), connectionSlot.getEventType(),
                connectionSlot.getPartition());
        final String zkPath = format("{0}/{1}", parent, connectionSlot.getConnectionId());
        try {
            zkHolder.get()
                    .delete()
                    .guaranteed()
                    .forPath(zkPath);
        } catch (final Exception e) {
            LOG.error("Zookeeper error when deleting consumer node", e);
        }
    }

    private String acquireConnection(final String client, final String eventType, final String partition) {
        final String parent = zkPathForConsumer(client, eventType, partition);
        final String connectionId = UUID.randomUUID().toString();
        final String zkPath = format("{0}/{1}", parent, connectionId);
        try {
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(zkPath);
        } catch (Exception e) {
            LOG.error("Zookeeper error when creating consumer node", e);
            throw new NakadiRuntimeException(e);
        }
        return connectionId;
    }

    private boolean connectionAllowed(final String client, final String eventType, final String partition) {
        final String zkPath = zkPathForConsumer(client, eventType, partition);
        try {
            final List<String> children = zkHolder.get()
                    .getChildren()
                    .forPath(zkPath);
            return children == null || children.size() < maxConnections;
        } catch (final KeeperException.NoNodeException nne) {
            return true;
        } catch (Exception e) {
            LOG.error("Zookeeper error when getting consumer nodes", e);
            throw new NakadiRuntimeException(e);
        }
    }

    private String zkPathForConsumer(final String client, final String eventType, final String partition) {
        return format("/nakadi/consumers/connections/{0}|{1}|{2}", client, eventType, partition);
    }

}
