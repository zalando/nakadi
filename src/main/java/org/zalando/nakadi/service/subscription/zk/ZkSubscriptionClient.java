package org.zalando.nakadi.service.subscription.zk;

import java.util.Collection;
import java.util.Map;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

public interface ZkSubscriptionClient {

    /**
     * Makes runLocked on subscription, using zk path /nakadi/locks/subscription_{subscriptionId}
     * Lock is created as an ephemeral node, so it will be deleted if nakadi go down. After obtaining runLocked,
     * provided function will be called under subscription runLocked
     *
     * @param function Function to call in context of runLocked.
     */
    void runLocked(Runnable function);

    /**
     * Checks if path /nakadi/subscriptions/{subscriptionId} exists in zookeeper
     *
     * @return true if exists, false otherwise
     * @throws Exception
     */
    boolean isSubscriptionCreated() throws Exception;

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
     * Deletes subscription wiht all its data in zookeeper
     */
    void deleteSubscription();

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
     * @param cursors Data to use for subscription filling.
     */
    void fillEmptySubscription(Collection<NakadiCursor> cursors);


    /**
     * Updates specified partition in zk.
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

    ZKSubscription subscribeForOffsetChanges(TopicPartition key, Runnable commitListener);

    /**
     * Returns current offset value for specified partition key.
     *
     * @param key Key to get offset for
     * @return commit offset
     */
    String getOffset(TopicPartition key);

    /**
     * Registers client connection using session id in /nakadi/subscriptions/{subscriptionId}/sessions/{session.id}
     * and value {{session.weight}}
     *
     * @param session Session to register.
     */
    void registerSession(Session session);

    void unregisterSession(Session session);

    /**
     * Transfers partitions to next client using data in zk. Updates topology_version if needed.
     *
     * @param sessionId  Someone who actually tries to transfer data.
     * @param partitions topic ids and partition ids of transferred data.
     */
    void transfer(String sessionId, Collection<TopicPartition> partitions);

    ZkSubscriptionNode getZkSubscriptionNodeLocked();
}
