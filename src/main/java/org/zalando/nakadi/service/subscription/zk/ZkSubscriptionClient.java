package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collection;
import java.util.Comparator;
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
    boolean isSubscriptionCreatedAndInitialized() throws NakadiRuntimeException;

    /**
     * Deletes subscription with all its data in zookeeper
     */
    void deleteSubscription();

    /**
     * Fills subscription object using partitions information provided (mapping from (topic, partition) to real offset
     * (NOT "BEGIN").
     *
     * @param cursors Data to use for subscription filling.
     */
    void fillEmptySubscription(Collection<SubscriptionCursorWithoutToken> cursors);

    /**
     * Updates specified partitions in zk.
     */
    void updatePartitionsConfiguration(Partition[] partitions) throws NakadiRuntimeException,
            SubscriptionNotInitializedException;

    /**
     * Returns session list in zk related to this subscription.
     *
     * @return List of existing sessions.
     */
    Session[] listSessions() throws SubscriptionNotInitializedException;

    boolean isActiveSession(String streamId) throws ServiceUnavailableException;

    /**
     * List partitions
     *
     * @return list of partitions related to this subscription.
     */
    Partition[] listPartitions() throws SubscriptionNotInitializedException, NakadiRuntimeException;

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

    ZKSubscription subscribeForOffsetChanges(EventTypePartition key, Runnable commitListener);

    /**
     * Returns current offset value for specified partition key. Offset includes timeline and version data.
     * The value that is stored there is a view value, so it will look like 001-0001-00000000000000000001
     *
     * @param key Key to get offset for
     * @return commit offset
     */
    SubscriptionCursorWithoutToken getOffset(EventTypePartition key) throws NakadiRuntimeException;

    List<Boolean> commitOffsets(List<SubscriptionCursorWithoutToken> cursors,
                                Comparator<SubscriptionCursorWithoutToken> comparator);

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
    void transfer(String sessionId, Collection<EventTypePartition> partitions)
            throws NakadiRuntimeException, SubscriptionNotInitializedException;

    /**
     * Retrieves subscription data like partitions and sessions from ZK under lock.
     *
     * @return list of partitions and sessions wrapped in
     * {@link org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode}
     */
    ZkSubscriptionNode getZkSubscriptionNodeLocked() throws SubscriptionNotInitializedException;

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
     * @param cursors cursors to reset to
     * @param timeout wait until give up resetting
     * @throws OperationTimeoutException
     * @throws ZookeeperException
     */
    void resetCursors(List<SubscriptionCursorWithoutToken> cursors, long timeout)
            throws OperationTimeoutException, ZookeeperException;
}
