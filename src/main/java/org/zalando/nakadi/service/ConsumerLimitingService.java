package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.ConnectionSlotOccupiedException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.text.MessageFormat.format;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Service
public class ConsumerLimitingService {

    public static final String CONNECTIONS_ZK_PATH = "/nakadi/consumers/connections";

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLimitingService.class);

    private static final String ERROR_MSG = "You exceeded the maximum number of simultaneous connections to a single " +
            "partition for event type: {0}, partition(s): {1}; max limit is {2} connections per client";

    private final ZooKeeperHolder zkHolder;
    private final int maxConnections;
    private final List<String> slotNames;

    @Autowired
    public ConsumerLimitingService(final ZooKeeperHolder zkHolder,
                                   @Value("${nakadi.stream.maxConnections}") final int maxConnections) {
        this.zkHolder = zkHolder;
        this.maxConnections = maxConnections;

        slotNames = IntStream.range(0, maxConnections)
                .boxed()
                .map(String::valueOf)
                .collect(Collectors.toList());
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
                    LOG.info("Failed to capture consuming connection slot after 5 tries for client '{}', " +
                            "event-type '{}', partition {}", client, eventType, partition);
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
        List<String> children;
        try {
            children = zkHolder.get()
                    .getChildren()
                    .forPath(parent);
        } catch (final KeeperException.NoNodeException e) {
            children = ImmutableList.of();
        } catch (Exception e) {
            LOG.error("Zookeeper error when getting consumer nodes", e);
            throw new NakadiRuntimeException(e);
        }

        if (children.size() >= maxConnections) {
            return Optional.empty();
        }

        final List<String> occupiedSlots = children;
        final List<String> availableSlots = slotNames.stream()
                .filter(slot -> !occupiedSlots.contains(slot))
                .collect(Collectors.toList());
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
        return Optional.of(new ConnectionSlot(client, eventType, partition, slot));
    }

    private String zkPathForConsumer(final String client, final String eventType, final String partition) {
        return CONNECTIONS_ZK_PATH + "/" + zkNodeNameForConsumer(client, eventType, partition);
    }

    private String zkNodeNameForConsumer(final String client, final String eventType, final String partition) {
        return format("{0}|{1}|{2}", client, eventType, partition);
    }

}
