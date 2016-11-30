package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.ConnectionSlotOccupiedException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;
import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.BUILD_INITIAL_CACHE;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class ConsumerLimitingService {

    public static final String CONNECTIONS_ZK_PATH = "/nakadi/consumers/connections";

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLimitingService.class);

    private static final String ERROR_MSG = "You exceeded the maximum number of simultaneous connections to a single " +
            "partition for event type: {0}, partition(s): {1}; max limit is {2} connections per client";

    private final ZooKeeperHolder zkHolder;
    private final int maxConnections;
    private final List<String> slotNames;

    private static final ConcurrentMap<String, PathChildrenCache> SLOTS_CACHES = Maps.newConcurrentMap();
    private static final List<ConnectionSlot> ACQUIRED_SLOTS = Collections.synchronizedList(Lists.newArrayList());

    @Autowired
    public ConsumerLimitingService(final ZooKeeperHolder zkHolder,
                                   @Value("${nakadi.stream.maxConnections}") final int maxConnections) {
        this.zkHolder = zkHolder;
        this.maxConnections = maxConnections;

        slotNames = IntStream.range(0, maxConnections)
                .boxed()
                .map(String::valueOf)
                .collect(toList());
    }

    @SuppressWarnings("unchecked")
    public List<ConnectionSlot> acquireConnectionSlots(final String client, final String eventType,
                                                       final List<String> partitions)
            throws NoConnectionSlotsException, ServiceUnavailableException {

        final List<Optional<ConnectionSlot>> slots = new ArrayList<>();
        try {
            // acquire slots for all partitions
            for (final String partition : partitions) {
                Optional<ConnectionSlot> connectionSlot = Optional.empty();
                try {
                    connectionSlot = executeWithRetry(
                            () -> acquireConnectionSlot(client, eventType, partition),
                            new RetryForSpecifiedCountStrategy<Optional<ConnectionSlot>>(maxConnections)
                                    .withExceptionsThatForceRetry(ConnectionSlotOccupiedException.class)
                                    .withWaitBetweenEachTry(0L, 100L));
                } catch (ConnectionSlotOccupiedException e) {
                    LOG.info("Failed to capture consuming connection slot after {} tries for client '{}', " +
                            "event-type '{}', partition {}", maxConnections, client, eventType, partition);
                }
                slots.add(connectionSlot);
            }

            if (slots.stream().allMatch(Optional::isPresent)) {
                // if slots for all partitions were acquired - the connection is successful
                return slots.stream()
                        .map(Optional::get)
                        .collect(Collectors.toList());
            } else {
                // if a slot for at least one partition wasn't acquired - connection can't be created
                final String failedPartitions = partitions.stream()
                        .filter(p -> !slots.stream().anyMatch(s -> s.isPresent() && s.get().getPartition().equals(p)))
                        .collect(Collectors.joining(", "));
                final String msg = format(ERROR_MSG, eventType, failedPartitions, maxConnections);
                throw new NoConnectionSlotsException(msg);
            }
        } catch (final Exception e) {
            // in a case of failure release slots for partitions that already acquired slots
            slots.stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(this::releaseConnectionSlot);
            if (e instanceof NoConnectionSlotsException) {
                throw e;
            } else {
                throw new ServiceUnavailableException("Error communicating with zookeeper", e);
            }
        }
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
            final PathChildrenCache cache = SLOTS_CACHES.getOrDefault(consumerPath, null);
            if (cache != null) {
                SLOTS_CACHES.remove(consumerPath);
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

    private Optional<ConnectionSlot> acquireConnectionSlot(final String client, final String eventType,
                                                           final String partition)
            throws ConnectionSlotOccupiedException {

        final String parent = zkPathForConsumer(client, eventType, partition);

        final List<String> occupiedSlots = getChildrenCached(parent);
        if (occupiedSlots.size() >= maxConnections) {
            return Optional.empty();
        }

        final List<String> availableSlots = slotNames.stream()
                .filter(slot -> !occupiedSlots.contains(slot))
                .collect(toList());
        final int slotIndex = new Random().nextInt(availableSlots.size());
        final String slot = availableSlots.get(slotIndex);

        final String zkPath = parent + "/" + slot;
        try {
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(zkPath);
        } catch (final KeeperException.NodeExistsException e) {
            throw new ConnectionSlotOccupiedException();
        } catch (Exception e) {
            LOG.error("Zookeeper error when creating consumer node", e);
            throw new NakadiRuntimeException(e);
        }

        final ConnectionSlot acquiredSlot = new ConnectionSlot(client, eventType, partition, slot);
        ACQUIRED_SLOTS.add(acquiredSlot);
        return Optional.of(acquiredSlot);
    }

    private List<String> getChildrenCached(final String zkPath) {
        try {
            PathChildrenCache cache = SLOTS_CACHES.getOrDefault(zkPath, null);
            if (cache == null) {
                cache = new PathChildrenCache(zkHolder.get(), zkPath, false);
                cache.start(BUILD_INITIAL_CACHE);
                SLOTS_CACHES.put(zkPath, cache);
            }
            return cache.getCurrentData().stream()
                    .map(childData -> {
                        final String[] pathParts = childData.getPath().split("/");
                        return pathParts[pathParts.length - 1];
                    })
                    .collect(toList());
        } catch (Exception e) {
            LOG.error("Zookeeper error when getting consumer nodes", e);
            throw new NakadiRuntimeException(e);
        }
    }

    private String zkPathForConsumer(final String client, final String eventType, final String partition) {
        return CONNECTIONS_ZK_PATH + "/" + zkNodeNameForConsumer(client, eventType, partition);
    }

    private String zkNodeNameForConsumer(final String client, final String eventType, final String partition) {
        return format("{0}|{1}|{2}", client, eventType, partition);
    }

}
