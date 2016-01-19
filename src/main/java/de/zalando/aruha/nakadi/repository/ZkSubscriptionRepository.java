package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.UUID;


public class ZkSubscriptionRepository implements SubscriptionRepository {

    private ZooKeeperHolder zooKeeperHolder;

    public ZkSubscriptionRepository(final ZooKeeperHolder zooKeeperHolder) {
        this.zooKeeperHolder = zooKeeperHolder;
    }

    @Override
    public void createSubscription(final String subscriptionId, final List<String> topics, final List<Cursor> cursors) {
        try {
            createPersistentEmptyNode("/nakadi/subscriptions/%s", subscriptionId);
            createPersistentEmptyNode("/nakadi/subscriptions/%s/topology", subscriptionId);
            for (final String topic : topics) {
                createPersistentEmptyNode("/nakadi/subscriptions/%s/topics/%s", subscriptionId, topic);
                createPersistentEmptyNode("/nakadi/subscriptions/%s/topics/%s/partitions", subscriptionId, topic);
            }
            for (final Cursor cursor: cursors) {
                final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                        cursor.getTopic(), cursor.getPartition());
                zooKeeperHolder
                        .get()
                        .create()
                        .forPath(path, cursor.getOffset().getBytes());
            }
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to create subscription in ZooKeeper", e);
        }
    }

    @Override
    public List<String> getSubscriptionTopics(final String subscriptionId) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics", subscriptionId);
            return zooKeeperHolder
                    .get()
                    .getChildren()
                    .forPath(path);
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to get topics of subscription in ZooKeeper", e);
        }
    }

    @Override
    public Cursor getCursor(final String subscriptionId, final String topic, final String partition) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                    topic, partition);
            final byte[] data = zooKeeperHolder
                    .get()
                    .getData()
                    .forPath(path);
            return new Cursor(topic, partition, new String(data));
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to get cursor of subscription in ZooKeeper", e);
        }
    }

    @Override
    public void saveCursor(final String subscriptionId, final Cursor cursor) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                    cursor.getTopic(), cursor.getPartition());
            zooKeeperHolder
                    .get()
                    .setData()
                    .forPath(path, cursor.getOffset().getBytes());
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to save cursor of subscription in ZooKeeper", e);
        }
    }

    @Override
    public String generateNewClientId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void addClient(final String subscriptionId, final String clientId) {
        try {
            final String pathToCreate = String.format("/nakadi/subscriptions/%s/topology/%s", subscriptionId, clientId);
            zooKeeperHolder
                    .get()
                    .create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(pathToCreate);
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to add client to subscription in ZooKeeper", e);
        }
    }

    @Override
    public void removeClient(final String subscriptionId, final String clientId) {
        try {
            final String pathToRemove = String.format("/nakadi/subscriptions/%s/topology/%s", subscriptionId, clientId);
            zooKeeperHolder.get().delete().forPath(pathToRemove);
        } catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to remove client of subscription in ZooKeeper", e);
        }
    }

    @Override
    public Topology getTopology(final String subscriptionId) {
        try {
            final String topologyPath = String.format("/nakadi/subscriptions/%s/topology", subscriptionId);
            final List<String> clientIds = zooKeeperHolder
                    .get()
                    .getChildren()
                    .forPath(topologyPath);
            return new Topology(clientIds);
        }
        catch (Exception e) {
            throw new NakadiRuntimeException("Error when tried to read subscription topology in ZooKeeper", e);
        }
    }

    private void createPersistentEmptyNode(final String pathTemplate, final Object... params) throws Exception {
        final String pathToCreate = params.length == 0 ? pathTemplate : String.format(pathTemplate, params);
        zooKeeperHolder
                .get()
                .create()
                .creatingParentsIfNeeded()
                .forPath(pathToCreate);
    }
}
