package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.NakadiWrapperException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.SubscriptionNotInitializedException;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.TimeLogger;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class CursorsService {

    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeCache eventTypeCache;
    private final NakadiSettings nakadiSettings;
    private final SubscriptionClientFactory zkSubscriptionFactory;
    private final CursorConverter cursorConverter;
    private final UUIDGenerator uuidGenerator;
    private final TimelineService timelineService;

    @Autowired
    public CursorsService(final SubscriptionDbRepository subscriptionRepository,
                          final EventTypeCache eventTypeCache,
                          final NakadiSettings nakadiSettings,
                          final SubscriptionClientFactory zkSubscriptionFactory,
                          final CursorConverter cursorConverter,
                          final UUIDGenerator uuidGenerator,
                          final TimelineService timelineService) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeCache = eventTypeCache;
        this.nakadiSettings = nakadiSettings;
        this.zkSubscriptionFactory = zkSubscriptionFactory;
        this.cursorConverter = cursorConverter;
        this.uuidGenerator = uuidGenerator;
        this.timelineService = timelineService;
    }

    /**
     * It is guaranteed, that len(cursors) == len(result)
     **/
    public List<Boolean> commitCursors(final String streamId, final String subscriptionId,
                                       final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException, InvalidStreamIdException,
            NoSuchEventTypeException, InternalNakadiException, NoSuchSubscriptionException, UnableProcessException {
        TimeLogger.addMeasure("getSubscription");
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);

        TimeLogger.addMeasure("validateSubscriptionCursors");
        validateSubscriptionCommitCursors(subscription, cursors);

        TimeLogger.addMeasure("createSubscriptionClient");
        final ZkSubscriptionClient zkClient = zkSubscriptionFactory.createClient(
                subscription, "subscription." + subscriptionId + "." + streamId + ".offsets");

        TimeLogger.addMeasure("validateStreamId");
        validateStreamId(cursors, streamId, zkClient);

        TimeLogger.addMeasure("writeToZK");
        return zkClient.commitOffsets(
                cursors.stream().map(cursorConverter::convertToNoToken).collect(Collectors.toList()),
                new SubscriptionCursorComparator(new NakadiCursorComparator(eventTypeCache)));
    }

    private void validateStreamId(final List<NakadiCursor> cursors, final String streamId,
                                  final ZkSubscriptionClient subscriptionClient)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException, InvalidStreamIdException {

        if (!uuidGenerator.isUUID(streamId)) {
            throw new InvalidStreamIdException(
                    String.format("Stream id has to be valid UUID, but `%s was provided", streamId), streamId);
        }

        if (!subscriptionClient.isActiveSession(streamId)) {
            throw new InvalidStreamIdException("Session with stream id " + streamId + " not found", streamId);
        }

        final Map<EventTypePartition, String> partitionSessions = Stream
                .of(subscriptionClient.getTopology().getPartitions())
                .filter(p -> p.getSession() != null)
                .collect(Collectors.toMap(Partition::getKey, Partition::getSession));
        for (final NakadiCursor cursor : cursors) {
            final EventTypePartition etPartition = cursor.getEventTypePartition();
            final String partitionSession = partitionSessions.get(etPartition);
            if (partitionSession == null) {
                throw new InvalidCursorException(CursorError.PARTITION_NOT_FOUND, cursor);
            }

            if (!streamId.equals(partitionSession)) {
                throw new InvalidStreamIdException("Cursor " + cursor + " cannot be committed with stream id "
                        + streamId, streamId);
            }
        }
    }

    public List<SubscriptionCursorWithoutToken> getSubscriptionCursors(final String subscriptionId)
            throws InternalNakadiException, ServiceTemporarilyUnavailableException {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final ZkSubscriptionClient zkSubscriptionClient = zkSubscriptionFactory.createClient(
                subscription, "subscription." + subscriptionId + ".get_cursors");
        final ImmutableList.Builder<SubscriptionCursorWithoutToken> cursorsListBuilder = ImmutableList.builder();

        Partition[] partitions;
        try {
            partitions = zkSubscriptionClient.getTopology().getPartitions();
        } catch (final SubscriptionNotInitializedException ex) {
            partitions = new Partition[]{};
        }
        final Map<EventTypePartition, SubscriptionCursorWithoutToken> positions = zkSubscriptionClient.getOffsets(
                Stream.of(partitions).map(Partition::getKey).collect(Collectors.toList()));

        for (final Partition p : partitions) {
            cursorsListBuilder.add(positions.get(p.getKey()));
        }
        return cursorsListBuilder.build();
    }

    public void resetCursors(final String subscriptionId, final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, NoSuchSubscriptionException,
            UnableProcessException, OperationTimeoutException, ZookeeperException,
            InternalNakadiException, NoSuchEventTypeException, InvalidCursorException {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        validateCursorsBelongToSubscription(subscription, cursors);
        for (final NakadiCursor cursor : cursors) {
            cursor.checkStorageAvailability();
        }

        final Map<TopicRepository, List<NakadiCursor>> topicRepositories = cursors.stream().collect(
                Collectors.groupingBy(
                        c -> timelineService.getTopicRepository(c.getTimeline())));
        for (final Map.Entry<TopicRepository, List<NakadiCursor>> entry : topicRepositories.entrySet()) {
            entry.getKey().validateReadCursors(entry.getValue());
        }

        final ZkSubscriptionClient zkClient = zkSubscriptionFactory.createClient(
                subscription, "subscription." + subscriptionId + ".reset_cursors");

        // In case if subscription was never initialized - initialize it
        zkClient.runLocked(() -> StartingState.initializeSubscriptionLocked(
                zkClient, subscription, timelineService, cursorConverter));
        // add 1 second to commit timeout in order to give time to finish reset if there is uncommitted events
        if (!cursors.isEmpty()) {
            final long timeout = TimeUnit.SECONDS.toMillis(nakadiSettings.getDefaultCommitTimeoutSeconds()) +
                    TimeUnit.SECONDS.toMillis(1);
            zkClient.resetCursors(
                    cursors.stream().map(cursorConverter::convertToNoToken).collect(Collectors.toList()),
                    timeout);
        }
    }

    private void validateSubscriptionCommitCursors(final Subscription subscription, final List<NakadiCursor> cursors)
            throws UnableProcessException {
        validateCursorsBelongToSubscription(subscription, cursors);

        cursors.forEach(cursor -> {
            try {
                cursor.checkStorageAvailability();
            } catch (final InvalidCursorException e) {
                throw new UnableProcessException(e.getMessage(), e);
            }
        });
    }

    private void validateCursorsBelongToSubscription(final Subscription subscription, final List<NakadiCursor> cursors)
            throws UnableProcessException {
        final List<String> wrongEventTypes = cursors.stream()
                .map(NakadiCursor::getEventType)
                .filter(et -> !subscription.getEventTypes().contains(et))
                .collect(Collectors.toList());
        if (!wrongEventTypes.isEmpty()) {
            throw new UnableProcessException("Event type does not belong to subscription: " + wrongEventTypes);
        }
    }

    private class SubscriptionCursorComparator implements Comparator<SubscriptionCursorWithoutToken> {
        private final Map<SubscriptionCursorWithoutToken, NakadiCursor> cached = new HashMap<>();
        private final Comparator<NakadiCursor> comparator;

        private SubscriptionCursorComparator(final Comparator<NakadiCursor> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(final SubscriptionCursorWithoutToken c1, final SubscriptionCursorWithoutToken c2) {
            return comparator.compare(convert(c1), convert(c2));
        }

        private NakadiCursor convert(final SubscriptionCursorWithoutToken value) {
            NakadiCursor result = cached.get(value);
            if (null != result) {
                return result;
            }
            try {
                result = cursorConverter.convert(value);
                cached.put(value, result);
                return result;
            } catch (final Exception ignore) {
                //On this stage exception should not be generated - cursors are validated.
                throw new NakadiWrapperException(ignore);
            }
        }
    }
}
