package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.OperationTimeoutException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.SubscriptionNotInitializedException;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class CursorsService {

    private final NakadiSettings nakadiSettings;
    private final SubscriptionClientFactory zkSubscriptionFactory;
    private final CursorConverter cursorConverter;
    private final UUIDGenerator uuidGenerator;
    private final TimelineService timelineService;
    private final AuthorizationValidator authorizationValidator;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionCache subscriptionCache;
    private final NakadiAuditLogPublisher auditLogPublisher;

    @Autowired
    public CursorsService(final SubscriptionDbRepository subscriptionRepository,
                          final SubscriptionCache subscriptionCache,
                          final NakadiSettings nakadiSettings,
                          final SubscriptionClientFactory zkSubscriptionFactory,
                          final CursorConverter cursorConverter,
                          final UUIDGenerator uuidGenerator,
                          final TimelineService timelineService,
                          final AuthorizationValidator authorizationValidator,
                          final NakadiAuditLogPublisher auditLogPublisher) {
        this.nakadiSettings = nakadiSettings;
        this.zkSubscriptionFactory = zkSubscriptionFactory;
        this.cursorConverter = cursorConverter;
        this.uuidGenerator = uuidGenerator;
        this.timelineService = timelineService;
        this.authorizationValidator = authorizationValidator;
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionCache = subscriptionCache;
        this.auditLogPublisher = auditLogPublisher;
    }

    /**
     * It is guaranteed, that len(cursors) == len(result)
     **/
    public List<Boolean> commitCursors(final String streamId, final String subscriptionId,
                                       final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException, InvalidStreamIdException,
            NoSuchEventTypeException, InternalNakadiException, NoSuchSubscriptionException, UnableProcessException,
            AccessDeniedException {
        try {
            final Subscription subscription = subscriptionCache.getSubscription(subscriptionId);
            authorizationValidator.authorizeSubscriptionView(subscription);
            authorizationValidator.authorizeSubscriptionCommit(subscription);
            validateSubscriptionCommitCursors(subscription, cursors);
            try (ZkSubscriptionClient zkClient = zkSubscriptionFactory.createClient(subscription)) {
                validateStreamId(cursors, streamId, zkClient, subscriptionId);
                return zkClient.commitOffsets(
                        cursors.stream().map(cursorConverter::convertToNoToken).collect(Collectors.toList()));
            }
        } catch (IOException io) {
            throw new ServiceTemporarilyUnavailableException(io.getMessage(), io);
        } catch (Exception e) {
            TracingService.logError(e.getMessage());
            throw e;
        }
    }

    private void validateStreamId(final List<NakadiCursor> cursors,
                                  final String streamId,
                                  final ZkSubscriptionClient subscriptionClient,
                                  final String subscriptionId)
            throws ServiceTemporarilyUnavailableException,
            InvalidCursorException,
            InvalidStreamIdException,
            SubscriptionNotInitializedException {

        if (!uuidGenerator.isUUID(streamId)) {
            final String error = String.format("Stream id has to be valid UUID, but `%s was provided", streamId);
            TracingService.logError(error);
            throw new InvalidStreamIdException(error, streamId);
        }

        if (!subscriptionClient.isActiveSession(streamId)) {
            final String error = String.format("Session with stream id %s not found", streamId);
            TracingService.logError(error);
            subscriptionCache.invalidateSubscription(subscriptionId);
            throw new InvalidStreamIdException(error, streamId);
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
                final String error = String.format(
                        "Cursor %s cannot be committed with stream id %s", cursor, streamId);
                TracingService.logError(error);
                throw new InvalidStreamIdException(error, streamId);
            }
        }
    }

    public List<SubscriptionCursorWithoutToken> getSubscriptionCursorsForUpdate(final String subscriptionId)
            throws InternalNakadiException, NoSuchEventTypeException,
            NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        return getSubscriptionCursors(subscriptionRepository.getSubscription(subscriptionId));
    }

    public List<SubscriptionCursorWithoutToken> getSubscriptionCursors(final String subscriptionId)
            throws InternalNakadiException, NoSuchEventTypeException,
            NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        return getSubscriptionCursors(subscriptionCache.getSubscription(subscriptionId));
    }

    private List<SubscriptionCursorWithoutToken> getSubscriptionCursors(final Subscription subscription)
            throws InternalNakadiException, NoSuchEventTypeException,
            NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        authorizationValidator.authorizeSubscriptionView(subscription);
        final ZkSubscriptionClient zkSubscriptionClient = zkSubscriptionFactory.createClient(subscription);
        try {
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
        } finally {
            try {
                zkSubscriptionClient.close();
            } catch (IOException io) {
                throw new ServiceTemporarilyUnavailableException(io.getMessage(), io);
            }
        }
    }

    public void resetCursors(final String subscriptionId,
                             final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, NoSuchSubscriptionException,
            UnableProcessException, OperationTimeoutException, ZookeeperException,
            InternalNakadiException, NoSuchEventTypeException, InvalidCursorException {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);

        authorizationValidator.authorizeSubscriptionView(subscription);
        authorizationValidator.authorizeSubscriptionAdmin(subscription);

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

        final ZkSubscriptionClient zkClient = zkSubscriptionFactory.createClient(subscription);
        try {
            // In case if subscription was never initialized - initialize it
            SubscriptionInitializer.initialize(
                    zkClient, subscription, timelineService, cursorConverter);
            // add 1 second to commit timeout in order to give time to finish reset if there is uncommitted events
            if (!cursors.isEmpty()) {
                final List<SubscriptionCursorWithoutToken> oldCursors = getSubscriptionCursorsForUpdate(subscriptionId);

                final long timeout = TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout()) +
                        TimeUnit.SECONDS.toMillis(1);
                final List<SubscriptionCursorWithoutToken> newCursors = cursors.stream()
                        .map(cursorConverter::convertToNoToken)
                        .collect(Collectors.toList());

                zkClient.closeSubscriptionStreams(() -> zkClient.forceCommitOffsets(newCursors), timeout);

                auditLogPublisher.publish(
                        Optional.of(new ItemsWrapper<>(oldCursors)),
                        Optional.of(new ItemsWrapper<>(newCursors)),
                        NakadiAuditLogPublisher.ResourceType.CURSORS,
                        NakadiAuditLogPublisher.ActionType.UPDATED,
                        subscriptionId);
            }
        } finally {
            try {
                zkClient.close();
            } catch (IOException io) {
                throw new ServiceTemporarilyUnavailableException(io.getMessage(), io);
            }
        }
    }

    private void validateSubscriptionCommitCursors(final Subscription subscription,
                                                   final List<NakadiCursor> cursors)
            throws UnableProcessException {
        validateCursorsBelongToSubscription(subscription, cursors);

        cursors.forEach(cursor -> {
            try {
                cursor.checkStorageAvailability();
            } catch (final InvalidCursorException e) {
                TracingService.logError(e.getMessage());
                throw new UnableProcessException(e.getMessage(), e);
            }
        });
    }

    private void validateCursorsBelongToSubscription(final Subscription subscription,
                                                     final List<NakadiCursor> cursors)
            throws UnableProcessException {
        final List<String> wrongEventTypes = cursors.stream()
                .map(NakadiCursor::getEventType)
                .filter(et -> !subscription.getEventTypes().contains(et))
                .collect(Collectors.toList());
        if (!wrongEventTypes.isEmpty()) {
            TracingService.logError("Event type does not belong to subscription: " + wrongEventTypes);
            throw new UnableProcessException("Event type does not belong to subscription: " + wrongEventTypes);
        }
    }
}
