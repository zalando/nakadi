package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;
import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.BUILD_INITIAL_CACHE;
import static org.zalando.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;

@Component
public class ConsumerLimitingService {

    public static final String CONNECTIONS_ZK_PATH = "/nakadi/consumers/connections";
    public static final String LOCKS_ZK_PATH = "/nakadi/consumers/locks";

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLimitingService.class);

    private static final String ERROR_MSG = "You exceeded the maximum number of simultaneous connections to a single " +
            "partition for event type: {0}, partition(s): {1}; max limit is {2} connections per client";

    private final ZooKeeperHolder zkHolder;
    private final ZooKeeperLockFactory zkLockFactory;
    private final int maxConnections;

    private static final ConcurrentMap<String, PathChildrenCache> SLOTS_CACHES = Maps.newConcurrentMap();
    private static final List<ConnectionSlot> ACQUIRED_SLOTS = Collections.synchronizedList(Lists.newArrayList());

    @Autowired
    public ConsumerLimitingService(final ZooKeeperHolder zkHolder,
                                   final ZooKeeperLockFactory zkLockFactory,
                                   @Value("${nakadi.stream.maxConnections}") final int maxConnections) {
        this.zkHolder = zkHolder;
        this.zkLockFactory = zkLockFactory;
        this.maxConnections = maxConnections;
    }

    @SuppressWarnings("unchecked")
    public List<ConnectionSlot> acquireConnectionSlots(final String client, final String eventType,
                                                       final List<String> partitions)
            throws NoConnectionSlotsException, ServiceUnavailableException {

        final List<String> partitionsWithNoFreeSlots = getPartitionsWithNoFreeSlots(client, eventType, partitions);
        if (partitionsWithNoFreeSlots.size() == 0) {

            final List<ConnectionSlot> slots = new ArrayList<>();
            final String lockZkPath = format("{0}/{1}|{2}", LOCKS_ZK_PATH, client, eventType);
            try {
                return runLocked(() -> {
                    // we need to check it again when we are under lock
                    final List<String> occupiedPartitions = getPartitionsWithNoFreeSlots(client, eventType, partitions);
                    if (occupiedPartitions.size() > 0) {
                        throw generateNoConnectionSlotsException(eventType, partitionsWithNoFreeSlots);
                    }

                    for (final String partition : partitions) {
                        final ConnectionSlot connectionSlot = acquireConnectionSlot(client, eventType, partition);
                        slots.add(connectionSlot);
                    }
                    return slots;
                }, zkLockFactory.createLock(lockZkPath));
            } catch (final NoConnectionSlotsException e) {
                throw e;
            } catch (final Exception e) {
                // in a case of failure release slots for partitions that already acquired slots
                slots.forEach(this::releaseConnectionSlot);
                throw new ServiceUnavailableException("Error communicating with zookeeper", e);
            }
        } else {
            throw generateNoConnectionSlotsException(eventType, partitionsWithNoFreeSlots);
        }
    }

    private List<String> getPartitionsWithNoFreeSlots(final String client, final String eventType,
                                                      final List<String> partitions) {
        return partitions.stream()
                .filter(partition -> {
                    final String zkPath = zkPathForConsumer(client, eventType, partition);
                    final List<String> slotsOccupied = getChildrenCached(zkPath);
                    return slotsOccupied.size() >= maxConnections;
                })
                .collect(toList());
    }

    private NoConnectionSlotsException generateNoConnectionSlotsException(final String eventType,
                                                                          final List<String> overBookedPartitions) {
        final String msg = format(ERROR_MSG, eventType, overBookedPartitions, maxConnections);
        return new NoConnectionSlotsException(msg);
    }

    public void releaseConnectionSlots(final List<ConnectionSlot> connectionSlots) {
        connectionSlots.forEach(this::releaseConnectionSlot);
    }

    private void releaseConnectionSlot(final ConnectionSlot slot) {
        final String consumerNode = zkNodeNameForConsumer(slot.getClient(), slot.getEventType(), slot.getPartition());
        final String connectionNodePath = format("{0}/{1}/{2}",
                CONNECTIONS_ZK_PATH, consumerNode, slot.getConnectionId());
        try {
            zkHolder.get()
                    .delete()
                    .guaranteed()
                    .forPath(connectionNodePath);
            deletePartitionNodeIfPossible(consumerNode);
        } catch (final Exception e) {
            LOG.error("Zookeeper error when deleting consumer connection node", e);
        }

        ACQUIRED_SLOTS.remove(slot);
        try {
            deleteCacheIfPossible(slot);
        } catch (final Exception e) {
            LOG.error("Zookeeper error when deleting consumer connections cache", e);
        }
    }

    private void deleteCacheIfPossible(final ConnectionSlot slot) throws IOException {
        final boolean hasMoreConnectionsToPartition = ACQUIRED_SLOTS.stream()
                .anyMatch(s -> s.getPartition().equals(slot.getPartition())
                        && s.getClient().equals(slot.getClient())
                        && s.getEventType().equals(slot.getEventType()));
        if (!hasMoreConnectionsToPartition) {
            final String consumerPath = zkPathForConsumer(slot.getClient(), slot.getEventType(), slot.getPartition());
            final PathChildrenCache cache = SLOTS_CACHES.remove(consumerPath);
            if (cache != null) {
                cache.close();
            }
        }
    }

    public void deletePartitionNodeIfPossible(final String nodeName) {
        try {
            zkHolder.get()
                    .delete()
                    .forPath(CONNECTIONS_ZK_PATH + "/" + nodeName);
        } catch (final KeeperException.NotEmptyException e) {
            // if the node has children - we should not delete it
        } catch (final Exception e) {
            LOG.error("Zookeeper error when trying delete consumer node", e);
        }
    }

    private ConnectionSlot acquireConnectionSlot(final String client, final String eventType,
                                                 final String partition) {

        final String parent = zkPathForConsumer(client, eventType, partition);
        final String slotId = UUID.randomUUID().toString();
        final String zkPath = parent + "/" + slotId;
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
        final ConnectionSlot acquiredSlot = new ConnectionSlot(client, eventType, partition, slotId);
        ACQUIRED_SLOTS.add(acquiredSlot);
        return acquiredSlot;
    }

    private List<String> getChildrenCached(final String zkPath) {
        final PathChildrenCache cache = SLOTS_CACHES.computeIfAbsent(zkPath, key -> {
            try {
                final PathChildrenCache newCache = new PathChildrenCache(zkHolder.get(), key, false);
                newCache.start(BUILD_INITIAL_CACHE);
                return newCache;
            } catch (final Exception e) {
                LOG.error("Zookeeper error when getting consumer nodes", e);
                throw new NakadiRuntimeException(e);
            }
        });
        return cache.getCurrentData().stream()
                .map(childData -> {
                    final String[] pathParts = childData.getPath().split("/");
                    return pathParts[pathParts.length - 1];
                })
                .collect(toList());
    }

    private String zkPathForConsumer(final String client, final String eventType, final String partition) {
        return CONNECTIONS_ZK_PATH + "/" + zkNodeNameForConsumer(client, eventType, partition);
    }

    private String zkNodeNameForConsumer(final String client, final String eventType, final String partition) {
        return format("{0}|{1}|{2}", client, eventType, partition);
    }

}
