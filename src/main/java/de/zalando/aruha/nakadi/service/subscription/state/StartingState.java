package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.exceptions.NoStreamingSlotsAvailable;
import de.zalando.aruha.nakadi.service.subscription.StreamingContext;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartingState extends State {
    private static final Logger LOG = LoggerFactory.getLogger(StartingState.class);

    public StartingState(final StreamingContext context) {
        super(context);
    }

    @Override
    public void onEnter() {
        context.zkClient.lock(this::createSubscriptionLocked);
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
        if (context.zkClient.createSubscription()) {
            // if not - create subscription node etc.
            context.zkClient.fillEmptySubscription(context.kafkaClient.getSubscriptionOffsets());
        } else {
            final Session[] sessions = context.zkClient.listSessions();
            final Partition[] partitions = context.zkClient.listPartitions();
            if (sessions == null || partitions == null) {
                context.switchState(new CleanupState(context));
                return;
            }
            if (sessions.length >= partitions.length) {
                context.switchState(new CleanupState(context, new NoStreamingSlotsAvailable(partitions.length)));
                return;
            }
        }

        if (!context.registerSession()) {
            context.switchState(new CleanupState(context));
            return;
        }
        try {
            context.out.onInitialized();
            context.switchState(new StreamingState(context));
        } catch (final IOException e) {
            LOG.error("Failed to notify of initialization. Switch to cleanup directly", e);
            context.switchState(new CleanupState(context, e));
        }
    }
}
