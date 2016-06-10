package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.exceptions.NoStreamingSlotsAvailable;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartingState extends State {
    private static final Logger LOG = LoggerFactory.getLogger(StartingState.class);

    @Override
    public void onEnter() {
        getZk().runLocked(this::createSubscriptionLocked);
    }

    /**
     * 1. Checks, that subscription node is present in zk. If not - creates it.
     * <p>
     * 2. Registers session.
     * <p>
     * 3. Switches to streaming state.
     */
    private void createSubscriptionLocked() {
        // check that subscription initialized in zk.
        if (getZk().createSubscription()) {
            // if not - create subscription node etc.
            getZk().fillEmptySubscription(getKafka().getSubscriptionOffsets());
        } else {
            final Session[] sessions = getZk().listSessions();
            final Partition[] partitions = getZk().listPartitions();
            if (sessions == null || partitions == null) {
                switchState(new CleanupState());
                return;
            }
            if (sessions.length >= partitions.length) {
                switchState(new CleanupState(new NoStreamingSlotsAvailable(partitions.length)));
                return;
            }
        }

        if (!registerSession()) {
            switchState(new CleanupState());
            return;
        }
        try {
            getOut().onInitialized();
            switchState(new StreamingState());
        } catch (final IOException e) {
            LOG.error("Failed to notify of initialization. Switch to cleanup directly", e);
            switchState(new CleanupState(e));
        }
    }
}
