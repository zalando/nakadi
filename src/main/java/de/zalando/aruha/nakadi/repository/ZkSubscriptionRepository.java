package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


public class ZkSubscriptionRepository implements SubscriptionRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ZkSubscriptionRepository.class);

    private ZooKeeperHolder zooKeeperHolder;

    public ZkSubscriptionRepository(final ZooKeeperHolder zooKeeperHolder) {
        this.zooKeeperHolder = zooKeeperHolder;
    }

    @PostConstruct
    private void createBasePath() {
        try {
            createPersistentEmptyNode("/nakadi");
            createPersistentEmptyNode("/nakadi/subscriptions");
        }
        catch(KeeperException.NodeExistsException e) {
            LOG.info("Base path for Nakadi already exists in ZooKeeper");
        } catch (InterruptedException | KeeperException e) {
            LOG.error("Error occurred when tried to create base path for Nakadi in ZooKeeper", e);
        }

    }

    @Override
    public void createSubscription(final String subscriptionId, final List<String> topics, final List<Cursor> cursors) {
        try {
            createPersistentEmptyNode("/nakadi/subscriptions/%s", subscriptionId);
            createPersistentEmptyNode("/nakadi/subscriptions/%s/topology", subscriptionId);
            createPersistentEmptyNode("/nakadi/subscriptions/%s/topics", subscriptionId);
            for (final String topic : topics) {
                createPersistentEmptyNode("/nakadi/subscriptions/%s/topics/%s", subscriptionId, topic);
                createPersistentEmptyNode("/nakadi/subscriptions/%s/topics/%s/partitions", subscriptionId, topic);
            }
            for (final Cursor cursor: cursors) {
                final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                        cursor.getTopic(), cursor.getPartition());
                zooKeeperHolder.get().create(path, cursor.getOffset().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        }
        catch (KeeperException | InterruptedException e) {
            throw new NakadiRuntimeException("Error when tried to create subscription in ZooKeeper", e);
        }
    }

    @Override
    public List<String> getSubscriptionTopics(final String subscriptionId) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics", subscriptionId);
            return zooKeeperHolder.get().getChildren(path, false);
        }
        catch (KeeperException | InterruptedException e) {
            throw new NakadiRuntimeException("Error when tried to get topics of subscription in ZooKeeper", e);
        }
    }

    @Override
    public Cursor getCursor(final String subscriptionId, final String topic, final String partition) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                    topic, partition);
            final byte[] data = zooKeeperHolder.get().getData(path, false, null);
            return new Cursor(topic, partition, new String(data));
        }
        catch (KeeperException | InterruptedException e) {
            throw new NakadiRuntimeException("Error when tried to get cursor of subscription in ZooKeeper", e);
        }
    }

    @Override
    public void saveCursor(final String subscriptionId, final Cursor cursor) {
        try {
            final String path = String.format("/nakadi/subscriptions/%s/topics/%s/partitions/%s", subscriptionId,
                    cursor.getTopic(), cursor.getPartition());
            zooKeeperHolder.get().setData(path, cursor.getOffset().getBytes(), -1);
        }
        catch (KeeperException | InterruptedException e) {
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
            zooKeeperHolder.get().create(pathToCreate, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        catch (KeeperException | InterruptedException e) {
            throw new NakadiRuntimeException("Error when tried to add client to subscription in ZooKeeper", e);
        }
    }

    @Override
    public void removeClient(final String subscriptionId, final String clientId) {
        try {
            final String pathToRemove = String.format("/nakadi/subscriptions/%s/topology/%s", subscriptionId, clientId);
            zooKeeperHolder.get().delete(pathToRemove, -1);
        } catch (InterruptedException | KeeperException e) {
            throw new NakadiRuntimeException("Error when tried to remove client of subscription in ZooKeeper", e);
        }
    }

    @Override
    public Topology getTopology(final String subscriptionId) {
        try {
            final String topologyPath = String.format("/nakadi/subscriptions/%s/topology", subscriptionId);
            final List<String> clientIds = zooKeeperHolder.get().getChildren(topologyPath, false);
            return new Topology(clientIds);
        }
        catch (KeeperException | InterruptedException e) {
            throw new NakadiRuntimeException("Error when tried to read subscription topology in ZooKeeper", e);
        }
    }

    private void createPersistentEmptyNode(final String pathTemplate, final String... params)
            throws KeeperException, InterruptedException {
        final String pathToCreate = params.length == 0 ? pathTemplate : String.format(pathTemplate, params);
        zooKeeperHolder.get().create(pathToCreate, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}
