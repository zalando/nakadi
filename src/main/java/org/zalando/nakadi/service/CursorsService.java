package org.zalando.nakadi.service;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CursorCommitResult;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class CursorsService {

    private static final Logger LOG = LoggerFactory.getLogger(CursorsService.class);

    private static final String PATH_ZK_OFFSET = "/nakadi/subscriptions/{0}/topics/{1}/{2}/offset";
    private static final String PATH_ZK_PARTITION = "/nakadi/subscriptions/{0}/topics/{1}/{2}";
    private static final String PATH_ZK_PARTITIONS = "/nakadi/subscriptions/{0}/topics/{1}";
    private static final String PATH_ZK_SESSION = "/nakadi/subscriptions/{0}/sessions/{1}";

    private static final String ERROR_COMMUNICATING_WITH_ZOOKEEPER = "Error communicating with zookeeper";

    private static final int COMMIT_CONFLICT_RETRY_TIMES = 5;

    private final ZooKeeperHolder zkHolder;
    private final TimelineService timelineService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeRepository eventTypeRepository;
    private final CursorTokenService cursorTokenService;
    private final CursorConverter cursorConverter;

    @Autowired
    public CursorsService(final ZooKeeperHolder zkHolder,
                          final TimelineService timelineService,
                          final SubscriptionDbRepository subscriptionRepository,
                          final EventTypeRepository eventTypeRepository,
                          final CursorTokenService cursorTokenService,
                          final CursorConverter cursorConverter) {
        this.zkHolder = zkHolder;
        this.timelineService = timelineService;
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.cursorTokenService = cursorTokenService;
        this.cursorConverter = cursorConverter;
    }

    public List<CursorCommitResult> commitCursors(final String streamId, final String subscriptionId,
                                                  final List<SubscriptionCursor> cursors)
            throws ServiceUnavailableException, InvalidCursorException, InvalidStreamIdException,
            NoSuchEventTypeException, InternalNakadiException {

        validateStreamId(cursors, streamId, subscriptionId);

        LOG.debug("[COMMIT_CURSORS] stream IDs validation finished");

        final Map<EventTypePartition, List<SubscriptionCursor>> cursorsByPartition = cursors.stream()
                .collect(Collectors.groupingBy(
                        cursor -> new EventTypePartition(cursor.getEventType(), cursor.getPartition())));

        final HashMap<EventTypePartition, Iterator<Boolean>> partitionCommits = new HashMap<>();
        for (final EventTypePartition etPartition : cursorsByPartition.keySet()) {

            final Iterator<Boolean> commitResultIterator = processPartitionCursors(subscriptionId,
                    cursorsByPartition.get(etPartition), etPartition).iterator();
            partitionCommits.put(etPartition, commitResultIterator);

            LOG.debug("[COMMIT_CURSORS] committed {} cursor(s) for partition {}",
                    cursorsByPartition.get(etPartition).size(), etPartition);
        }

        return cursors.stream()
                .map(cursor -> {
                    final EventTypePartition etPartition = new EventTypePartition(cursor.getEventType(),
                            cursor.getPartition());
                    final Boolean committed = partitionCommits.get(etPartition).next();
                    return new CursorCommitResult(cursor, committed);
                })
                .collect(Collectors.toList());
    }

    private void validateStreamId(final List<SubscriptionCursor> cursors, final String streamId,
                                  final String subscriptionId)
            throws ServiceUnavailableException, InvalidCursorException, InvalidStreamIdException,
            NoSuchEventTypeException, InternalNakadiException {

        if (!isActiveSession(subscriptionId, streamId)) {
            throw new InvalidStreamIdException("Session with stream id " + streamId + " not found");
        }

        final HashMap<EventTypePartition, String> partitionSessions = new HashMap<>();
        for (final SubscriptionCursor cursor : cursors) {
            final EventType eventType = eventTypeRepository.findByName(cursor.getEventType());

            final EventTypePartition etPartition = new EventTypePartition(eventType.getName(), cursor.getPartition());
            String partitionSession = partitionSessions.get(etPartition);
            if (partitionSession == null) {
                partitionSession = getPartitionSession(subscriptionId, eventType.getTopic(), cursor);
                partitionSessions.put(etPartition, partitionSession);
            }

            if (!streamId.equals(partitionSession)) {
                throw new InvalidStreamIdException("Cursor " + cursor + " cannot be committed with stream id "
                        + streamId);
            }
        }
    }

    private String getPartitionSession(final String subscriptionId, final String topic, final Cursor cursor)
            throws ServiceUnavailableException, InvalidCursorException {
        try {
            final String partitionPath = format(PATH_ZK_PARTITION, subscriptionId, topic, cursor.getPartition());
            final byte[] partitionData = zkHolder.get().getData().forPath(partitionPath);
            final Partition.PartitionKey partitionKey = new Partition.PartitionKey(topic, cursor.getPartition());
            return CuratorZkSubscriptionClient.deserializeNode(partitionKey, partitionData).getSession();
        } catch (final KeeperException.NoNodeException e) {
            throw new InvalidCursorException(CursorError.PARTITION_NOT_FOUND, cursor);
        } catch (final Exception e) {
            LOG.error(ERROR_COMMUNICATING_WITH_ZOOKEEPER, e);
            throw new ServiceUnavailableException(ERROR_COMMUNICATING_WITH_ZOOKEEPER);
        }
    }

    private boolean isActiveSession(final String subscriptionId, final String streamId)
            throws ServiceUnavailableException {
        try {
            final String sessionsPath = format(PATH_ZK_SESSION, subscriptionId, streamId);
            return zkHolder.get().checkExists().forPath(sessionsPath) != null;
        } catch (final Exception e) {
            LOG.error(ERROR_COMMUNICATING_WITH_ZOOKEEPER, e);
            throw new ServiceUnavailableException(ERROR_COMMUNICATING_WITH_ZOOKEEPER);
        }
    }

    private List<Boolean> processPartitionCursors(final String subscriptionId, final List<SubscriptionCursor> cursors,
                                                  final EventTypePartition eventTypePartition)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException,
            InvalidCursorException {

        final EventType eventType = eventTypeRepository.findByName(eventTypePartition.getEventType());

        try {
            final List<NakadiCursor> nakadiCursors = cursors.stream()
                    .map(cursor -> {
                        SubscriptionCursor cursorToProcess = cursor;
                        if (Cursor.BEFORE_OLDEST_OFFSET.equals(cursor.getOffset())) {
                            cursorToProcess = new SubscriptionCursor(cursor.getPartition(), "-1", cursor.getEventType(),
                                    cursor.getCursorToken());
                        }
                        final NakadiCursor nakadiCursor = new NakadiCursor(
                                eventType.getTopic(),
                                cursorToProcess.getPartition(),
                                cursorToProcess.getOffset());
                        try {
                            timelineService.getTopicRepository(eventType).validateCommitCursor(nakadiCursor);
                            return nakadiCursor;
                        } catch (final InvalidCursorException e) {
                            throw new NakadiRuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());

            LOG.debug("[COMMIT_CURSORS] finished validation of {} cursor(s) for partition {}", cursors.size(),
                    eventTypePartition);

            return commitPartitionCursors(subscriptionId, eventType, nakadiCursors, eventTypePartition);

        } catch (final NakadiRuntimeException e) {
            throw (InvalidCursorException) e.getException();
        }
    }

    private List<Boolean> commitPartitionCursors(final String subscriptionId,
                                                 final EventType eventType,
                                                 final List<NakadiCursor> cursors,
                                                 final EventTypePartition etPartition)
            throws ServiceUnavailableException {
        final String topic = eventType.getTopic();
        final String offsetPath = format(PATH_ZK_OFFSET, subscriptionId, topic, etPartition.getPartition());
        final TopicRepository topicRepository = timelineService.getTopicRepository(eventType);
        try {
            @SuppressWarnings("unchecked")
            final List<Boolean> committed = executeWithRetry(() -> {
                        final Stat stat = new Stat();
                        final byte[] currentOffsetData = zkHolder.get()
                                .getData()
                                .storingStatIn(stat)
                                .forPath(offsetPath);
                        final String currentOffset = new String(currentOffsetData, Charsets.UTF_8);
                        NakadiCursor currentMaxCursor = new NakadiCursor(topic, etPartition.getPartition(),
                                currentOffset);

                        final List<Boolean> commits = Lists.newArrayList();
                        for (final NakadiCursor cursor : cursors) {
                            if (topicRepository.compareOffsets(cursor, currentMaxCursor) > 0) {
                                currentMaxCursor = cursor;
                                commits.add(true);
                            } else {
                                commits.add(false);
                            }
                        }

                        if (!currentMaxCursor.getOffset().equals(currentOffset)) {
                            zkHolder.get()
                                    .setData()
                                    .withVersion(stat.getVersion())
                                    .forPath(offsetPath, currentMaxCursor.getOffset().getBytes(Charsets.UTF_8));
                        }
                        return commits;
                    },
                    new RetryForSpecifiedCountStrategy<List<Boolean>>(COMMIT_CONFLICT_RETRY_TIMES)
                            .withExceptionsThatForceRetry(KeeperException.BadVersionException.class));

            return Optional.ofNullable(committed)
                    .orElse(Collections.nCopies(cursors.size(), false));
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
            final NakadiCursor nakadiCursor = new NakadiCursor(
                    topic,
                    partition,
                    new String(zkHolder.get().getData().forPath(offsetPath), Charsets.UTF_8));
            return cursorConverter.convert(nakadiCursor, eventType, cursorTokenService.generateToken());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
