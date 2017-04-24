package org.zalando.nakadi.service.subscription.zk;

import java.util.Collection;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import java.util.List;

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
     * Creates subscription node in zookeeper on path /nakadi/subscriptions/{subscriptionId}
     *
     * @return true if subscription was created. False if subscription already present. To operate on this value
     * additional field 'state' is used /nakadi/subscriptions/{subscriptionId}/state. Just after creation it has value
     * CREATED. After {{@link #fillEmptySubscription}} call it will have value INITIALIZED. So true
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

    /**
     * Retrieves subscription data like partitions and sessions from ZK under lock.
     *
     * @return list of partitions and sessions wrapped in
     * {@link org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode}
     */
    ZkSubscriptionNode getZkSubscriptionNodeLocked();

    /**
     * Subscribes to cursor reset event.
     *
     * @param listener callback which is called when cursor reset happens
     * @return {@link org.zalando.nakadi.service.subscription.zk.ZKSubscription}
     */
    ZKSubscription subscribeForCursorsReset(Runnable listener)
            throws NakadiRuntimeException, UnsupportedOperationException;

    /**
     * Gets current status of cursor reset request.
     *
     * @return true if cursor reset in progress
     */
    boolean isCursorResetInProgress();

    /**
     * Resets subscription offsets for provided cursors.
     *
     * @param cursors cursors to reset
     * @param timeout wait until give up resetting
     * @throws OperationTimeoutException
     * @throws ZookeeperException
     */
    void resetCursors(List<NakadiCursor> cursors, long timeout) throws OperationTimeoutException, ZookeeperException;
}
