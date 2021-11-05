package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.SubscriptionPartitionConflictException;
import org.zalando.nakadi.service.SubscriptionInitializer;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class StartingState extends State {
    @Override
    public void onEnter() {
        // 1. Check authorization
        getContext().registerForAuthorizationUpdates();
        try {
            getContext().checkAccessAuthorized();
        } catch (final AccessDeniedException e) {
            switchState(new CleanupState(e));
            return;
        }
        registerSessionAndStartStreaming();
    }

    /**
     * 1. Checks, that subscription node is present in zk. If not - creates it.
     * <p>
     * 2. If cursor reset is in progress it will switch to cleanup state.
     * <p>У
     * 3. Registers session.
     * <p>
     * 4. Switches to streaming state.
     */
    private void registerSessionAndStartStreaming() {
        SubscriptionInitializer.initialize(getZk(),
                getContext().getSubscription(),
                getContext().getTimelineService(),
                getContext().getCursorConverter());
        try {
            checkStreamingSlotsAvailable(getZk().listSessions());
        } catch (NoStreamingSlotsAvailable | SubscriptionPartitionConflictException ex) {
            switchState(new CleanupState(ex));
            return;
        }

        if (getZk().isCloseSubscriptionStreamsInProgress()) {
            logStreamCloseReason("Resetting subscription cursors request is still in progress");
            switchState(new CleanupState(
                    new ConflictException("Resetting subscription cursors request is still in progress")));
            return;
        }

        try {
            getContext().registerSession();
            checkStreamingSlotsAvailable(getZk().listSessions().stream()
                    .filter(s -> !s.getId().equals(getSessionId()))
                    .collect(Collectors.toList()));
        } catch (RuntimeException ex) {
            switchState(new CleanupState(ex));
            return;
        }

        switchState(new StreamingState());
    }

    private void checkStreamingSlotsAvailable(final Collection<Session> sessions)
            throws NoStreamingSlotsAvailable, SubscriptionPartitionConflictException {
        // check if there are streaming slots available
        final Partition[] partitions = getZk().getTopology().getPartitions();
        final List<EventTypePartition> requestedPartitions = getContext().getParameters().getPartitions();
        if (requestedPartitions.isEmpty()) {
            final long directlyRequestedPartitionsCount = sessions.stream()
                    .flatMap(s -> s.getRequestedPartitions().stream())
                    .distinct()
                    .count();
            final long autoSlotsCount = partitions.length - directlyRequestedPartitionsCount;
            final long autoBalanceSessionsCount = sessions.stream()
                    .filter(s -> s.getRequestedPartitions().isEmpty())
                    .count();

            if (autoBalanceSessionsCount >= autoSlotsCount) {
                logStreamCloseReason("No streaming slots available");
                throw new NoStreamingSlotsAvailable(getContext().getSubscription().getId(), partitions.length);
            }
        }

        // check if the requested partitions are not directly requested by another stream(s)
        final List<EventTypePartition> conflictPartitions = sessions.stream()
                .flatMap(s -> s.getRequestedPartitions().stream())
                .filter(requestedPartitions::contains)
                .collect(Collectors.toList());
        if (!conflictPartitions.isEmpty()) {
            logStreamCloseReason("Partition already taken by other stream of the subscription");
            throw SubscriptionPartitionConflictException.of(conflictPartitions);
        }
    }
}
