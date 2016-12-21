package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.runtime.Version;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.*;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.StorageWorker;
import org.zalando.nakadi.service.timeline.StorageWorkerFactory;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;
import static org.zalando.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;

@Component
public class CursorsService {

    private static final Logger LOG = LoggerFactory.getLogger(CursorsService.class);
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    private static final String PATH_ZK_OFFSET = "/nakadi/subscriptions/{0}/topics/{1}/{2}/offset";
    private static final String PATH_ZK_PARTITIONS = "/nakadi/subscriptions/{0}/topics/{1}";
    private static final String ERROR_COMMUNICATING_WITH_ZOOKEEPER = "Error communicating with zookeeper";

    private final ZooKeeperHolder zkHolder;
    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeRepository eventTypeRepository;
    private final ZooKeeperLockFactory zkLockFactory;
    private final ZkSubscriptionClientFactory zkSubscriptionClientFactory;
    private final CursorTokenService cursorTokenService;
    private final TimelineService timelineService;
    private final StorageWorkerFactory storageWorkerFactory;
    @Autowired
    public CursorsService(final ZooKeeperHolder zkHolder,
                          final SubscriptionDbRepository subscriptionRepository,
                          final EventTypeRepository eventTypeRepository,
                          final ZooKeeperLockFactory zkLockFactory,
                          final ZkSubscriptionClientFactory zkSubscriptionClientFactory,
                          final CursorTokenService cursorTokenService,
                          final TimelineService timelineService,
                          final StorageWorkerFactory storageWorkerFactory) {
        this.zkHolder = zkHolder;
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.zkLockFactory = zkLockFactory;
        this.zkSubscriptionClientFactory = zkSubscriptionClientFactory;
        this.cursorTokenService = cursorTokenService;
        this.timelineService = timelineService;
        this.storageWorkerFactory = storageWorkerFactory;
    }

    public Map<SubscriptionCursor, Boolean> commitCursors(final String streamId, final String subscriptionId,
                                                          final List<SubscriptionCursor> cursors)
            throws NakadiException, InvalidCursorException {

        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final ZkSubscriptionClient subscriptionClient = zkSubscriptionClientFactory.createZkSubscriptionClient(
                subscription.getId());

        validateCursors(subscriptionClient, cursors, streamId);

        return cursors.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        Try.<SubscriptionCursor, Boolean>wrap(cursor -> processCursor(subscriptionId, cursor))
                                .andThen(Try::getOrThrow)));
    }

    private void validateCursors(final ZkSubscriptionClient subscriptionClient,
                                 final List<SubscriptionCursor> cursors,
                                 final String streamId) throws NakadiException {
        final ZkSubscriptionNode subscription = subscriptionClient.getZkSubscriptionNodeLocked();
        if (!Arrays.stream(subscription.getSessions()).anyMatch(session -> session.getId().equals(streamId))) {
            throw new InvalidStreamIdException("Session with stream id " + streamId + " not found");
        }
        final Map<Partition.PartitionKey, Partition> partitions = Arrays.stream(subscription.getPartitions())
                .collect(Collectors.toMap(Partition::getKey, Function.identity()));
        final List<SubscriptionCursor> invalidCursors = cursors.stream()
                .filter(cursor ->
                        Try.cons(() -> eventTypeRepository.findByName(cursor.getEventType()))
                                .getO()
                                .map(eventType -> {
                                    final Partition.PartitionKey key =
                                            new Partition.PartitionKey(eventType.getTopic(), cursor.getPartition());
                                    final Partition partition = partitions.get(key);
                                    return partition == null || !streamId.equals(partition.getSession());
                                })
                                .orElseGet(() -> false))
                .collect(Collectors.toList());
        if (!invalidCursors.isEmpty()) {
            throw new InvalidStreamIdException("Cursors " + invalidCursors + " cannot be committed with stream id "
                    + streamId);
        }
    }

    private boolean processCursor(final String subscriptionId, final SubscriptionCursor cursor)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException,
            ServiceUnavailableException, NoSuchSubscriptionException {

        SubscriptionCursor cursorToProcess = cursor;
        if (Cursor.BEFORE_OLDEST_OFFSET.equals(cursor.getOffset())) {
            cursorToProcess = new SubscriptionCursor(cursor.getPartition(), "-1", cursor.getEventType(),
                    cursor.getCursorToken());
        }
        final EventType eventType = eventTypeRepository.findByName(cursorToProcess.getEventType());

        final VersionedCursor vCursor = VersionedCursor.restore(cursor);
        final Timeline timeline = timelineService.getTimeline(eventType, vCursor);
        storageWorkerFactory.getWorker(timeline.getStorage()).getTopicRepository().validateCommitCursors(
                timeline.getStorageConfiguration(),
                ImmutableList.of(cursorToProcess));

        return commitCursor(subscriptionId, eventType, vCursor, timeline);
    }

    private boolean commitCursor(final String subscriptionId, final EventType eventType,
                                 final VersionedCursor cursor, final Timeline timeline)
            throws ServiceUnavailableException, NoSuchSubscriptionException, InvalidCursorException {

        final String offsetPath = format(PATH_ZK_OFFSET, subscriptionId, eventType, cursor.getPartition());
        try {
            return runLocked(() -> {
                final String currentOffset = new String(zkHolder.get().getData().forPath(offsetPath), CHARSET_UTF8);
                final VersionedCursor currentCursor = VersionedCursor.restore(new Cursor(cursor.getPartition(), currentOffset));
                final Timeline currentTimeline = timelineService.getTimeline(eventType, currentCursor);
                if (currentTimeline.getOrder() > timeline.getOrder()) {
                    return false;
                }
                if (currentTimeline.getOrder().equals(timeline.getOrder()) &&
                        storageWorkerFactory.getWorker(timeline.getStorage()).compare(cursor, currentCursor) <= 0) {
                    return false;
                }
                zkHolder.get().setData().forPath(offsetPath, cursor.toCursor().getOffset().getBytes(CHARSET_UTF8));
                return true;
            }, zkLockFactory.createLock(offsetPath));
        } catch (final IllegalArgumentException e) {
            throw new InvalidCursorException(CursorError.INVALID_FORMAT, cursor.toCursor());
        } catch (final Exception e) {
            throw new ServiceUnavailableException(ERROR_COMMUNICATING_WITH_ZOOKEEPER, e);
        }
    }

    public List<SubscriptionCursor> getSubscriptionCursors(final String subscriptionId) throws NakadiException {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final ImmutableList.Builder<SubscriptionCursor> cursorsListBuilder = ImmutableList.builder();

        for (final String eventType : subscription.getEventTypes()) {

            final String topic = eventTypeRepository.findByName(eventType).getTopic();
            final String partitionsPath = format(PATH_ZK_PARTITIONS, subscriptionId, topic);
            try {
                final List<String> partitions = zkHolder.get().getChildren().forPath(partitionsPath);

                final List<SubscriptionCursor> eventTypeCursors = partitions.stream()
                        .map(partition -> readCursor(subscriptionId, topic, partition, eventType))
                        .collect(Collectors.toList());

                cursorsListBuilder.addAll(eventTypeCursors);
            } catch (final KeeperException.NoNodeException nne) {
                LOG.debug(nne.getMessage(), nne);
                return Collections.emptyList();
            } catch (final Exception e) {
                LOG.error(e.getMessage(), e);
                throw new ServiceUnavailableException(ERROR_COMMUNICATING_WITH_ZOOKEEPER, e);
            }
        }
        return cursorsListBuilder.build();
    }

    private SubscriptionCursor readCursor(final String subscriptionId, final String topic, final String partition,
                                          final String eventType)
            throws RuntimeException {
        try {
            final String offsetPath = format(PATH_ZK_OFFSET, subscriptionId, topic, partition);
            String currentOffset = new String(zkHolder.get().getData().forPath(offsetPath), CHARSET_UTF8);
            if ("-1".equals(currentOffset)) {
                currentOffset = Cursor.BEFORE_OLDEST_OFFSET;
            }
            return new SubscriptionCursor(partition, currentOffset, eventType, cursorTokenService.generateToken());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
