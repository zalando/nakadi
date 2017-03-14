package org.zalando.nakadi.service.subscription.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoStreamingSlotsAvailable;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.timeline.TimelineService;

public class StartingState extends State {
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
            final List<NakadiCursor> cursors = calculateStartPosition(
                    getContext().getSubscription(),
                    getContext().getTimelineService(),
                    getContext().getCursorConverter());
            getLog().info("Initial offsets are {}", Arrays.deepToString(cursors.toArray()));
            getZk().fillEmptySubscription(cursors);
        } else {
            final Session[] sessions = getZk().listSessions();
            final Partition[] partitions = getZk().listPartitions();
            if (sessions.length >= partitions.length) {
                switchState(new CleanupState(new NoStreamingSlotsAvailable(partitions.length)));
                return;
            }
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

    public interface PositionCalculator {
        Subscription.InitialPosition getType();

        List<NakadiCursor> calculate(
                Subscription subscription, TimelineService timelineService, CursorConverter converter);
    }

    public static class BeginPositionCalculator implements PositionCalculator {

        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.BEGIN;
        }

        @Override
        public List<NakadiCursor> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            final List<NakadiCursor> result = new ArrayList<>();
            try {
                for (final String eventType : subscription.getEventTypes()) {
                    final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                    final Timeline timeline = activeTimelines.get(0);
                    timelineService.getTopicRepository(timeline)
                            .loadTopicStatistics(Collections.singletonList(timeline))
                            .forEach(stat -> result.add(stat.getBeforeFirst()));
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
        public List<NakadiCursor> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            final List<NakadiCursor> result = new ArrayList<>();
            try {
                for (final String eventType : subscription.getEventTypes()) {
                    final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                    final Timeline timeline = activeTimelines.get(activeTimelines.size() - 1);
                    timelineService.getTopicRepository(timeline)
                            .loadTopicStatistics(Collections.singletonList(timeline))
                            .forEach(stat -> result.add(stat.getLast()));
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
        public List<NakadiCursor> calculate(
                final Subscription subscription,
                final TimelineService timelineService,
                final CursorConverter converter) {
            final List<NakadiCursor> result = new ArrayList<>();
            subscription.getInitialCursors().forEach(cursor -> {
                try {
                    result.add(converter.convert(cursor));
                } catch (final Exception e) {
                    throw new NakadiRuntimeException(e);
                }
            });
            return result;
        }
    }


    public static List<NakadiCursor> calculateStartPosition(
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
