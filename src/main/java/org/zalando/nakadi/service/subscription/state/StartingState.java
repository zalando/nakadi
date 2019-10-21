package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionPartitionConflictException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

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
        getZk().runLocked(this::initializeStream);
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
    private void initializeStream() {
        final boolean subscriptionJustInitialized = initializeSubscriptionLocked(getZk(),
                getContext().getSubscription(), getContext().getTimelineService(), getContext().getCursorConverter());
        getContext().getCurrentSpan().setTag("session.id", getContext().getSessionId());
        if (!subscriptionJustInitialized) {
            // check if there are streaming slots available
            final Collection<Session> sessions = getZk().listSessions();
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
                    TracingService.logStreamCloseReason(getContext().getCurrentSpan(), "No streaming slots available");
                    switchState(new CleanupState(new NoStreamingSlotsAvailable(partitions.length)));
                    return;
                }
            }

            // check if the requested partitions are not directly requested by another stream(s)
            final List<EventTypePartition> conflictPartitions = sessions.stream()
                    .flatMap(s -> s.getRequestedPartitions().stream())
                    .filter(requestedPartitions::contains)
                    .collect(Collectors.toList());
            if (!conflictPartitions.isEmpty()) {
                TracingService.logStreamCloseReason(getContext().getCurrentSpan(),
                        "Partition already taken by other stream of the subscription");
                switchState(new CleanupState(SubscriptionPartitionConflictException.of(conflictPartitions)));
                return;
            }
        }

        if (getZk().isCloseSubscriptionStreamsInProgress()) {
            TracingService.logStreamCloseReason(getContext().getCurrentSpan(),
                    "Resetting subscription cursors request is still in progress");
            switchState(new CleanupState(
                    new ConflictException("Resetting subscription cursors request is still in progress")));
            return;
        }

        try {
            getContext().registerSession();
        } catch (Exception ex) {
            switchState(new CleanupState(ex));
            return;
        }

        try {
            getOut().onInitialized(getSessionId());
            switchState(new StreamingState());
        } catch (final IOException e) {
            getLog().error("Failed to notify of initialization. Switch to cleanup directly", e);
            switchState(new CleanupState(e));
        }
    }

    public static boolean initializeSubscriptionLocked(
            final ZkSubscriptionClient zkClient,
            final Subscription subscription,
            final TimelineService timelineService,
            final CursorConverter cursorConverter) {
        if (!zkClient.isSubscriptionCreatedAndInitialized()) {
            final List<SubscriptionCursorWithoutToken> cursors = calculateStartPosition(
                    subscription, timelineService, cursorConverter);
            zkClient.fillEmptySubscription(cursors);
            return true;
        }
        return false;
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
