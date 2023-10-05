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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class SubscriptionInitializer {
    // TODO: should be configurable
    private static final String DLQ_EVENT_TYPE = "nakadi.dlq.event.type";
    
    public static void initialize(
            final ZkSubscriptionClient zkClient,
            final Subscription subscription,
            final TimelineService timelineService,
            final CursorConverter cursorConverter) {
        if (!zkClient.isSubscriptionCreatedAndInitialized()) {
            final List<SubscriptionCursorWithoutToken> cursors = calculateStartPosition(
                    subscription, timelineService, cursorConverter);
            zkClient.fillSubscription(cursors, false);
        }
        // TODO: feature toggle, check if subscription has opt-in attributes.
        if (isTestSubscription(subscription) && !isEventTypeInTopology(DLQ_EVENT_TYPE, zkClient.getTopology())) {
            final List<SubscriptionCursorWithoutToken> cursors = calculateDlqStartPosition(
                    timelineService, cursorConverter);
            zkClient.fillSubscription(cursors, true);
        }
    }

    private static boolean isTestSubscription(final Subscription subscription) {
        return List.of("95e293f0-48db-438d-b31c-09b7f9c244b2", "28f38c31-e980-4997-9069-b725ee077bfe")
                .contains(subscription.getId());
    }

    private static boolean isEventTypeInTopology(final String eventType, final ZkSubscriptionClient.Topology topology) {
        return Arrays.stream(topology.getPartitions())
                .anyMatch(partition -> partition.getEventType().equals(eventType));
    }

    private interface PositionCalculator {
        Subscription.InitialPosition getType();

        default List<SubscriptionCursorWithoutToken> calculate(
                Subscription subscription, TimelineService timelineService, CursorConverter converter) {
            return calculate(subscription.getEventTypes(), timelineService, converter);
        }

        List<SubscriptionCursorWithoutToken> calculate(
                Set<String> eventTypes, TimelineService timelineService, CursorConverter converter);
    }

    private static class BeginPositionCalculator implements PositionCalculator {

        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.BEGIN;
        }

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Set<String> eventTypes,
                final TimelineService timelineService,
                final CursorConverter converter) {
            return eventTypes
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

    private static class EndPositionCalculator implements PositionCalculator {
        @Override
        public Subscription.InitialPosition getType() {
            return SubscriptionBase.InitialPosition.END;
        }

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Set<String> eventTypes,
                final TimelineService timelineService,
                final CursorConverter converter) {
            return eventTypes
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

    private static class CursorsPositionCalculator implements PositionCalculator {
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

        @Override
        public List<SubscriptionCursorWithoutToken> calculate(
                final Set<String> eventTypes,
                final TimelineService timelineService,
                final CursorConverter converter) {
            throw new UnsupportedOperationException("CursorsPositionCalculator requires actual cursors configuration");
        }
    }

    public static List<SubscriptionCursorWithoutToken> calculateDlqStartPosition(
            final TimelineService timelineService,
            final CursorConverter converter
    ) {
        final var initialPosition = SubscriptionBase.InitialPosition.BEGIN;
        final PositionCalculator result = POSITION_CALCULATORS.get(initialPosition);
        if (null == result) {
            throw new RuntimeException("Position calculation for " + initialPosition + " is not supported");
        }
        return result.calculate(Collections.singleton(DLQ_EVENT_TYPE), timelineService, converter);
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

    private static void register(final PositionCalculator calculator) {
        POSITION_CALCULATORS.put(calculator.getType(), calculator);
    }

    static {
        register(new BeginPositionCalculator());
        register(new EndPositionCalculator());
        register(new CursorsPositionCalculator());
    }
}
