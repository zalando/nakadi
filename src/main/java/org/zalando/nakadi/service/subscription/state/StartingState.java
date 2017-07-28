package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class StartingState extends State {
    @Override
    public void onEnter() {
        // 1. Check authorization
        getContext().registerForAuthorizationUpdates();
        try {
            getContext().checkAccessAuthorized();
        } catch (final AccessDeniedException e) {
            switchState(new CleanupState(
                    new NakadiException(e.explain()) {
                        @Override
                        protected Response.StatusType getStatus() {
                            return Response.Status.FORBIDDEN;
                        }
                    }));
            return;
        }
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
        if (!getZk().isSubscriptionCreatedAndInitialized()) {
            final List<SubscriptionCursorWithoutToken> cursors = calculateStartPosition(
                    getContext().getSubscription(),
                    getContext().getTimelineService(),
                    getContext().getCursorConverter());
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

        getContext().registerSession();

        try {
            getOut().onInitialized(getSessionId());
            switchState(new StreamingState());
        } catch (final IOException e) {
            getLog().error("Failed to notify of initialization. Switch to cleanup directly", e);
            switchState(new CleanupState(e));
        }
    }

    public interface PositionCalculator {
        Subscription.InitialPosition getType();

        List<SubscriptionCursorWithoutToken> calculate(
                Subscription subscription, TimelineService timelineService, CursorConverter converter);
    }

    public static class BeginPositionCalculator implements PositionCalculator {

        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.BEGIN;
        }

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            final List<SubscriptionCursorWithoutToken> result = new ArrayList<>();
            try {
                for (final String eventType : subscription.getEventTypes()) {
                    final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                    final Timeline timeline = activeTimelines.get(0);
                    timelineService.getTopicRepository(timeline)
                            .loadTopicStatistics(Collections.singletonList(timeline))
                            .forEach(stat -> result.add(converter.convertToNoToken(stat.getBeforeFirst())));
                }
                return result;
            } catch (final Exception ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }

    public static class EndPositionCalculator implements PositionCalculator {
        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.END;
        }

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            final List<SubscriptionCursorWithoutToken> result = new ArrayList<>();
            try {
                for (final String eventType : subscription.getEventTypes()) {
                    final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                    final Timeline timeline = activeTimelines.get(activeTimelines.size() - 1);
                    timelineService.getTopicRepository(timeline)
                            .loadTopicStatistics(Collections.singletonList(timeline))
                            .forEach(stat -> result.add(converter.convertToNoToken(stat.getLast())));
                }
                return result;
            } catch (final Exception ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }

    public static class CursorsPositionCalculator implements PositionCalculator {
        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.CURSORS;
        }

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            return subscription.getInitialCursors();
        }
    }

    public static List<SubscriptionCursorWithoutToken> calculateStartPosition(
            final Subscription subscription,
            final TimelineService timelineService,
            final CursorConverter converter) {
        final PositionCalculator result = POSITION_CALCULATORS.get(subscription.getReadFrom());
        if (null == result) {
            throw new RuntimeException("Position calculation for " + subscription.getReadFrom() + " is not supported");
        }
        return result.calculate(subscription, timelineService, converter);
    }

    private static final Map<Subscription.InitialPosition, PositionCalculator> POSITION_CALCULATORS =
            new EnumMap<>(SubscriptionBase.InitialPosition.class);

    static void register(final PositionCalculator calculator) {
        POSITION_CALCULATORS.put(calculator.getType(), calculator);
    }

    static {
        register(new BeginPositionCalculator());
        register(new EndPositionCalculator());
        register(new CursorsPositionCalculator());
    }
}
