package org.zalando.nakadi.service;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.List;
import java.util.UUID;

import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;

@Service
public class ConsumerLimitingService {

    private static final Logger LOG = LoggerFactory.getLogger(CursorsService.class);

    private ZooKeeperHolder zkHolder;
    private int maxConnections;

    @Autowired
    public ConsumerLimitingService(final ZooKeeperHolder zkHolder,
                                   @Value("${nakadi.stream.maxConnections}") final int maxConnections) {
        this.zkHolder = zkHolder;
        this.maxConnections = maxConnections;
    }

    public List<String> acquireConnectionSlots(final String client, final String eventType,
                                               final List<String> partitions)
            throws NoConnectionSlotsException, ServiceUnavailableException {
        try {
            final List<String> notAllowed = partitions.stream()
                    .filter(partition -> !connectionAllowed(client, eventType, partition))
                    .collect(toList());
            if (!notAllowed.isEmpty()) {
                final String msg = format("You exceeded the maximum number of simultaneous connections to a single " +
                        "partition for event type '{0}', partition(s): {1}; max limit is {2} connections per client",
                        eventType, String.join(", ", notAllowed), maxConnections);
                throw new NoConnectionSlotsException(msg);
            }

            @SuppressWarnings("UnnecessaryLocalVariable")
            final List<String> connectionIds = partitions.stream()
                    .map(partition -> acquireConnection(client, eventType, partition))
                    .collect(toList());
            return connectionIds;
        } catch (final NakadiRuntimeException e) {
            throw new ServiceUnavailableException("Error communicating with zookeeper", e.getException());
        }
    }

    private void releaseConnection(final String client, final String eventType, final String partition,
                                   final String connectionId) {
        final String parent = zkPathForConsumer(client, eventType, partition);
        final String zkPath = format("{0}/{1}", parent, connectionId);
        try {
            zkHolder.get()
                    .delete()
                    .forPath(zkPath);
        } catch (final Exception e) {
            LOG.error("Zookeeper error deleting consumer node", e);
            throw new NakadiRuntimeException(e);
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
        } catch (Exception e) {
            LOG.error("Zookeeper error getting consumer nodes", e);
            throw new NakadiRuntimeException(e);
        }
    }

    private String zkPathForConsumer(final String client, final String eventType, final String partition) {
        return format("/nakadi/consumers/{0}|{1}|{2}", client, eventType, partition);
    }
}
