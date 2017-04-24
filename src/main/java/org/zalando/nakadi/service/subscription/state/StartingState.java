package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoStreamingSlotsAvailable;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class StartingState extends State {
    @Override
    public void onEnter() {
        getZk().runLocked(this::createSubscriptionLocked);
    }

    /**
     * 1. Checks, that subscription node is present in zk. If not - creates it.
     * <p>
     * 2. If cursor reset is in progress it will switch to cleanup state.
     * <p>
     * 3. Registers session.
     * <p>
     * 4. Switches to streaming state.
     */
    private void createSubscriptionLocked() {
        // check that subscription initialized in zk.
        if (getZk().createSubscription()) {
            // if not - create subscription node etc.
            final Map<Partition.PartitionKey, String> cursors = getKafka().getSubscriptionOffsets().entrySet()
                    .stream().collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> entry.getValue().getOffset()
                            )
                    );
            // TODO: On the very first stage when only zero-version cursors are used, it will work.
            // This should be fixed by using correct layering.
            getZk().fillEmptySubscription(cursors);
        } else {
            final Session[] sessions = getZk().listSessions();
            final Partition[] partitions = getZk().listPartitions();
            if (sessions.length >= partitions.length) {
                switchState(new CleanupState(new NoStreamingSlotsAvailable(partitions.length)));
                return;
            }
        }

        if (getZk().isCursorResetInProgress()) {
            switchState(new CleanupState(
                    new NakadiException("Resetting subscription cursors request is still in progress") {
                        @Override
                        protected Response.StatusType getStatus() {
                            return Response.Status.CONFLICT;
                        }
                    }));
            return;
        }

        registerSession();

        try {
            getOut().onInitialized(getSessionId());
            switchState(new StreamingState());
        } catch (final IOException e) {
            getLog().error("Failed to notify of initialization. Switch to cleanup directly", e);
            switchState(new CleanupState(e));
        }
    }

}
