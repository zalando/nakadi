package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class SubscriptionInitializer {
    
    public static void initialize(
            final ZkSubscriptionClient zkClient,
            final Subscription subscription,
            final TimelineService timelineService,
            final CursorConverter cursorConverter) {
        if (!zkClient.isSubscriptionCreatedAndInitialized()) {
            final List<SubscriptionCursorWithoutToken> cursors = calculateStartPosition(
                    subscription, timelineService, cursorConverter);
            zkClient.fillEmptySubscription(cursors);
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
            return subscription.getEventTypes()
                    .stream()
                    .map(et -> {
                        try {
                            // get oldest active timeline
                            return timelineService.getActiveTimelinesOrdered(et).get(0);
                        } catch (final InternalNakadiException e) {
                            throw new NakadiRuntimeException(e);
                        }
                    })
                    .collect(groupingBy(Timeline::getStorage)) // for performance reasons. See ARUHA-1387
                    .values()
                    .stream()
                    .flatMap(timelines -> {
                        try {
                            return timelineService.getTopicRepository(timelines.get(0))
                                    .loadTopicStatistics(timelines).stream();
                        } catch (final ServiceTemporarilyUnavailableException e) {
                            throw new NakadiRuntimeException(e);
                        }
                    })
                    .map(PartitionStatistics::getBeforeFirst)
                    .map(converter::convertToNoToken)
                    .collect(Collectors.toList());
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
            return subscription.getEventTypes()
                    .stream()
                    .map(et -> {
                        try {
                            // get newest active timeline
                            final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(et);
                            return activeTimelines.get(activeTimelines.size() - 1);
                        } catch (final InternalNakadiException e) {
                            throw new NakadiRuntimeException(e);
                        }
                    })
                    .collect(groupingBy(Timeline::getStorage)) // for performance reasons. See ARUHA-1387
                    .values()
                    .stream()
                    .flatMap(timelines -> {
                        try {
                            return timelineService.getTopicRepository(timelines.get(0))
                                    .loadTopicEndStatistics(timelines).stream();
                        } catch (final ServiceTemporarilyUnavailableException e) {
                            throw new NakadiRuntimeException(e);
                        }
                    })
                    .map(PartitionEndStatistics::getLast)
                    .map(converter::convertToNoToken)
                    .collect(Collectors.toList());
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
