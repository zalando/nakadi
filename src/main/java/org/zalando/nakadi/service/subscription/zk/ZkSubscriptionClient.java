package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.binary.Hex;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

public interface ZkSubscriptionClient {

    /**
     * Makes runLocked on subscription, using zk path /nakadi/locks/subscription_{subscriptionId}
     * Lock is created as an ephemeral node, so it will be deleted if nakadi goes down. After obtaining runLocked,
     * provided function will be called under subscription runLocked
     *
     * @param function Function to call in context of runLocked.
     */
    <T> T runLocked(Callable<T> function);

    default void runLocked(final Runnable function) {
        runLocked((Callable<Void>) () -> {
            function.run();
            return null;
        });
    }


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
    void updatePartitionsConfiguration(String newSessionsHash, Partition[] partitions) throws NakadiRuntimeException,
            SubscriptionNotInitializedException;

    /**
     * Returns session list in zk related to this subscription.
     *
     * @return List of existing sessions.
     */
    Collection<Session> listSessions()
            throws SubscriptionNotInitializedException, NakadiRuntimeException, ServiceTemporarilyUnavailableException;

    boolean isActiveSession(String streamId) throws ServiceTemporarilyUnavailableException;

    /**
     * List partitions
     *
     * @return list of partitions related to this subscription.
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
     * Retrieves subscription data like partitions and sessions from ZK without a lock
     *
     * @return list of partitions and sessions wrapped in
     * {@link org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode}
     */
    Optional<ZkSubscriptionNode> getZkSubscriptionNode()
            throws SubscriptionNotInitializedException, NakadiRuntimeException;

    /**
     * Subscribes to cursor reset event.
     *
     * @param listener callback which is called when cursor reset happens
     * @return {@link Closeable}
     */
    Closeable subscribeForCursorsReset(Runnable listener)
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
     * @throws OperationTimeoutException
     * @throws ZookeeperException
     */
    void resetCursors(List<SubscriptionCursorWithoutToken> cursors)
            throws OperationTimeoutException, ZookeeperException;

    class Topology {
        @JsonProperty("partitions")
        private final Partition[] partitions;
        // Each topology is based on a list of sessions, that it was built for.
        // In case, when list of sessions wasn't changed, one should not actually perform rebalance, cause nothing have
        // changed.
        @Nullable
        @JsonProperty("sessions_hash")
        private final String sessionsHash;
        @Nullable
        @JsonProperty("version")
        private final Integer version;

        public Topology(
                @JsonProperty("partitions") final Partition[] partitions,
                @Nullable @JsonProperty("sessions_hash") final String sessionsHash,
                @Nullable @JsonProperty("version") final Integer version) {
            this.partitions = partitions;
            this.sessionsHash = sessionsHash;
            this.version = version;
        }

        public Partition[] getPartitions() {
            return partitions;
        }

        public Topology withUpdatedPartitions(final String newHash, final Partition[] partitions) {
            final Partition[] resultPartitions = Arrays.copyOf(this.partitions, this.partitions.length);
            for (final Partition newValue : partitions) {
                int selectedIdx = -1;
                for (int idx = 0; idx < resultPartitions.length; ++idx) {
                    if (resultPartitions[idx].getKey().equals(newValue.getKey())) {
                        selectedIdx = idx;
                    }
                }
                if (selectedIdx < 0) {
                    throw new NakadiBaseException(
                            "Failed to find partition " + newValue.getKey() + " in " + this);
                }
                resultPartitions[selectedIdx] = newValue;
            }
            return new Topology(resultPartitions, newHash, Optional.ofNullable(version).orElse(0) + 1);
        }

        @Override
        public String toString() {
            return "Topology{" +
                    "partitions=" + Arrays.toString(partitions) +
                    ", sessionsHash=" + sessionsHash +
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
                    Arrays.equals(partitions, topology.partitions) &&
                    Objects.equals(sessionsHash, topology.sessionsHash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionsHash, version);
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

        public boolean isSameHash(final String newHash) {
            return Objects.equals(newHash, sessionsHash);
        }

        public String getSessionsHash() {
            return sessionsHash;
        }
    }
}
