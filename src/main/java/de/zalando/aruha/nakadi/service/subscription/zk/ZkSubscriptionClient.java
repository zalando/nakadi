package de.zalando.aruha.nakadi.service.subscription.zk;

import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

public interface ZkSubscriptionClient {

    interface ZKSubscription {
        void cancel();
    }

    /**
     * Makes lock on subscription, using zk path /nakadi/locks/subscription_{subscriptionId}
     * Lock is created as an ephemeral node, so it will be deleted if nakadi go down. After obtaining lock, provided
     * function will be called under subscription lock
     *
     * @param function Function to call in context of lock.
     */
    void lock(Runnable function);

    /**
     * Creates subscription node in zookeeper on path /nakadi/subscriptions/{subscriptionId}
     *
     * @return true if subscription was created. False if subscription already present. To operate on this value
     * additional field 'state' is used /nakadi/subscriptions/{subscriptionId}/state. Just after creation it has value
     * CREATED. After {{@link #fillEmptySubscription(Map)}} call it will have value INITIALIZED. So true
     * will be returned in case of state is equal to CREATED.
     */
    boolean createSubscription();

    /**
     * Fills subscription object using partitions information provided (mapping from (topic, partition) to real offset
     * (NOT "BEGIN"). Generated object in zk will look like this
     * nakadi
     * - {subscription_id}
     * |- state: INITIALIZED
     * |- event-types:
     * ||- et1:
     * |||- partitions:
     * || |- 0: {session: null, next_session: null, state: UNASSIGNED}
     * || ||- offset: {OFFSET}
     * || |- 1: {session: null, next_session: null, state: UNASSIGNED}
     * ||  |- offset: {OFFSET}
     * ||- et2:
     * | |-partitions:
     * |  |- 0: {session: null, next_session: null, state: UNASSIGNED}
     * |  ||- offset: {OFFSET}
     * |  |- 1: {session: null, next_session: null, state: UNASSIGNED}
     * |   |- offset: {OFFSET}
     *
     * @param partitions Data to use for subscription filling.
     */
    void fillEmptySubscription(Map<Partition.PartitionKey, Long> partitions);


    /**
     * Updates specified partition in zk.
     *
     */
    void updatePartitionConfiguration(Partition partition);

    /**
     * Increments value in /nakadi/subscriptions/{subscriptionId}/topology_version
     */
    void incrementTopology();


    /**
     * Returns session list in zk related to this subscription.
     *
     * @return List of existing sessions.
     */
    Session[] listSessions();

    /**
     * List partitions
     *
     * @return list of partitions related to this subscription.
     */
    Partition[] listPartitions();

    /**
     * Subscribes to changes of session list in /nakadi/subscriptions/{subscriptionId}/sessions.
     * Each time list is changed calls {{@code listener}} subscription.
     *
     * @param listener method to call on any change of client list.
     */
    ZKSubscription subscribeForSessionListChanges(Runnable listener);

    /**
     * Subscribe for topology changes.
     *
     * @param listener called whenever /nakadi/subscriptions/{subscriptionId}/topology node is changed.
     * @return Subscription instance
     */
    ZKSubscription subscribeForTopologyChanges(Runnable listener);

    ZKSubscription subscribeForOffsetChanges(Partition.PartitionKey key, Consumer<Long> commitListener);

    /**
     * Returns current offset value for specified partition key.
     * @param key Key to get offset for
     * @return commit offset
     */
    Long getOffset(Partition.PartitionKey key);

    /**
     * Registers client connection using session id in /nakadi/subscriptions/{subscriptionId}/sessions/{session.id} and value {{session.weight}}
     *
     * @param session Session to register.
     */
    boolean registerSession(Session session);

    void unregisterSession(Session session);

    /**
     * Transfers partitions to next client using data in zk. Updates topology_version if needed.
     *
     * @param sessionId   Someone who actually tries to transfer data.
     * @param partitions topic ids and partition ids of transferred data.
     */
    void transfer(String sessionId, Collection<Partition.PartitionKey> partitions);
}
