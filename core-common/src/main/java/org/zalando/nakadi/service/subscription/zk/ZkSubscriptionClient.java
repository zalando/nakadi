package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.binary.Hex;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

public interface ZkSubscriptionClient extends Closeable {

    /**
     * Creates subscription node in zookeeper on path /nakadi/subscriptions/{subscriptionId}
     *
     * @return true if subscription was created. False if subscription already present. To operate on this value
     * additional field 'state' is used /nakadi/subscriptions/{subscriptionId}/state. Just after creation it has value
     * CREATED. After {{@link #fillEmptySubscription}} call it will have value INITIALIZED. So true
     * will be returned in case of state is equal to INITIALIZED.
     */
    boolean isSubscriptionCreatedAndInitialized() throws NakadiRuntimeException;

    /**
     * Deletes subscription with all its data in zookeeper
     */
    void deleteSubscription();

    /**
     * Fills subscription object using partitions information provided (mapping from (topic, partition) to offset,
     * "BEGIN" is allowed.
     *
     * @param cursors Data to use for subscription filling.
     */
    void fillEmptySubscription(Collection<SubscriptionCursorWithoutToken> cursors);

    void createOffsetZNodes(Collection<SubscriptionCursorWithoutToken> cursors) throws Exception;

    /**
     * Updates topologies partitions by reading topology first and
     * then writing the change back with usage of zookeeper node version.
     * If zookeeper node version was changed in between it will retry
     * by reading new zookeeper node version.
     */
    void updateTopology(Function<Topology, Partition[]> partitioner)
            throws NakadiRuntimeException, SubscriptionNotInitializedException;

    /**
     * Returns session list in zk related to this subscription.
     *
     * @return List of existing sessions.
     */
    Collection<Session> listSessions()
            throws SubscriptionNotInitializedException, NakadiRuntimeException, ServiceTemporarilyUnavailableException;

    boolean isActiveSession(String streamId) throws ServiceTemporarilyUnavailableException;

    /**
     * Returns subscription {@link Topology} object from Zookeeper
     *
     * @return topology {@link Topology}
     * @throws SubscriptionNotInitializedException
     * @throws NakadiRuntimeException
     */
    Topology getTopology() throws SubscriptionNotInitializedException, NakadiRuntimeException;

    /**
     * Subscribes to changes of session list in /nakadi/subscriptions/{subscriptionId}/sessions.
     * Each time list is changed calls {{@code listener}} subscription.
     *
     * @param listener method to call on any change of client list.
     */
    ZkSubscription<List<String>> subscribeForSessionListChanges(Runnable listener) throws NakadiRuntimeException;

    /**
     * Subscribe for topology changes.
     *
     * @param listener called whenever /nakadi/subscriptions/{subscriptionId}/topology node is changed.
     * @return Subscription instance
     */
    ZkSubscription<Topology> subscribeForTopologyChanges(Runnable listener) throws NakadiRuntimeException;

    ZkSubscription<SubscriptionCursorWithoutToken> subscribeForOffsetChanges(
            EventTypePartition key, Runnable commitListener);

    /**
     * Returns committed offset values for specified partition keys.
     * Offsets include timeline and version data.
     * The value that is stored there is a view value, so it will look like
     * 001-0001-00000000000000000001
     *
     * @param keys Key to get offset for
     * @return commit offset
     */
    Map<EventTypePartition, SubscriptionCursorWithoutToken> getOffsets(Collection<EventTypePartition> keys)
            throws NakadiRuntimeException;

    /**
     * Forcefully set commits offsets specified in {@link SubscriptionCursorWithoutToken}
     *
     * @param cursors - offsets to set for subscription
     * @throws Exception
     */
    void forceCommitOffsets(List<SubscriptionCursorWithoutToken> cursors) throws NakadiRuntimeException;

    List<Boolean> commitOffsets(List<SubscriptionCursorWithoutToken> cursors);

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
     * Retrieves subscription data like partitions and sessions from ZK without a lock
     *
     * @return list of partitions and sessions wrapped in
     * {@link org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode}
     */
    Optional<ZkSubscriptionNode> getZkSubscriptionNode()
            throws SubscriptionNotInitializedException, NakadiRuntimeException;

    /**
     * Subscribes for subscription stream close event.
     *
     * @param listener callback which is called when stream is closed
     * @return {@link Closeable}
     */
    Closeable subscribeForStreamClose(Runnable listener)
            throws NakadiRuntimeException, UnsupportedOperationException;

    /**
     * Extends topology for subscription after event type partitions increased
     *
     * @param eventTypeName      Name of the event-type that was repartitioned
     * @param newPartitionsCount Count of the number of partitions of the event type after repartitioning
     * @param offset             Offset to start consume from, usually it is begin, but have to preserve timelines
     */
    void repartitionTopology(String eventTypeName, int newPartitionsCount, String offset)
            throws NakadiRuntimeException;

    /**
     * Gets current status of subscription stream closing.
     *
     * @return true if cursor reset in progress
     */
    boolean isCloseSubscriptionStreamsInProgress();

    /**
     * Close subscription streams and perform provided action when streams are closed.
     *
     * Specifically the steps taken are:
     *
     *   1. It creates a /subscriptions/{SID}/cursor_reset znode - thus signaling to all the
     *      consumers that they should terminate
     *   2. waits for the session count on this subscription to go down to zero
     *   3. executes the action
     *   4. deletes the /subscriptions/{SID}/cursor_reset znode - thus making the subscription available
     *      to the consumers again
     *
     * @param action  perform action once streams are closed
     * @param timeout maximum amount of time it will wait for the session count to go down to 0.
     *                If exceeded an OperationTimeoutException is thrown.
     * @throws OperationTimeoutException
     * @throws ZookeeperException
     */
    void closeSubscriptionStreams(Runnable action, long timeout)
            throws OperationTimeoutException, ZookeeperException;

    class Topology {
        @JsonProperty("partitions")
        private final Partition[] partitions;
        @Nullable
        @JsonProperty("version")
        private final Integer version;

        public Topology(
                @JsonProperty("partitions") final Partition[] partitions,
                @Nullable @JsonProperty("version") final Integer version) {
            this.partitions = partitions;
            this.version = version;
        }

        public Partition[] getPartitions() {
            return partitions;
        }

        public Topology withUpdatedPartitions(final Partition[] partitions) {
            final var resultPartitions = new ArrayList<>(Arrays.asList(this.partitions));
            for (final Partition newValue : partitions) {
                final var selected = IntStream.range(0, resultPartitions.size())
                        .filter(idx -> resultPartitions.get(idx).getKey().equals(newValue.getKey()))
                        .findFirst();
                if (selected.isPresent()) {
                    resultPartitions.set(selected.getAsInt(), newValue);
                } else {
                    resultPartitions.add(newValue);
                }
            }
            return new Topology(resultPartitions.toArray(new Partition[0]), Optional.ofNullable(version).orElse(0) + 1);
        }

        @Override
        public String toString() {
            return "Topology{" +
                    "partitions=" + Arrays.toString(partitions) +
                    ", version=" + version +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Topology topology = (Topology) o;
            return Objects.equals(version, topology.version) &&
                    Arrays.equals(partitions, topology.partitions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version);
        }

        public static String calculateSessionsHash(final Collection<String> sessionIds)
                throws ServiceTemporarilyUnavailableException {
            final MessageDigest md;
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new ServiceTemporarilyUnavailableException("hash algorithm not found", e);
            }
            sessionIds.stream().sorted().map(String::getBytes).forEach(md::update);
            final byte[] digest = md.digest();
            return Hex.encodeHexString(digest);
        }

        public Integer getVersion() {
            return version;
        }
    }
}
