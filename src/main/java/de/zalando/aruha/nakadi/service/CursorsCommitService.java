package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.CursorError;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperLockFactory;

import java.nio.charset.Charset;
import java.util.List;

import static de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;
import static java.text.MessageFormat.format;

public class CursorsCommitService {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final ZooKeeperHolder zkHolder;
    private final TopicRepository topicRepository;
    private final SubscriptionDbRepository subscriptionRepository;
    private final ZooKeeperLockFactory zkLockFactory;

    public CursorsCommitService(final ZooKeeperHolder zkHolder, final TopicRepository topicRepository,
                                final SubscriptionDbRepository subscriptionRepository,
                                final ZooKeeperLockFactory zkLockFactory) {
        this.zkHolder = zkHolder;
        this.topicRepository = topicRepository;
        this.subscriptionRepository = subscriptionRepository;
        this.zkLockFactory = zkLockFactory;
    }

    public boolean commitCursors(final String subscriptionId, final List<Cursor> cursors)
            throws InvalidCursorException, NoSuchSubscriptionException, ServiceUnavailableException {

        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        final String eventType = subscription.getEventTypes().iterator().next();

        topicRepository.validateCommitCursors(eventType, cursors);

        boolean allCommitted = true;
        for (final Cursor cursor : cursors) {
            allCommitted = allCommitted && commitCursor(subscriptionId, eventType, cursor);
        }
        return allCommitted;
    }

    private boolean commitCursor(final String subscriptionId, final String eventType, final Cursor cursor)
            throws ServiceUnavailableException, NoSuchSubscriptionException, InvalidCursorException {

        final String offsetPath = format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset",
                subscriptionId, eventType, cursor.getPartition());
        try {
            return runLocked(() -> {
                final String currentOffset = new String(zkHolder.get().getData().forPath(offsetPath), CHARSET);
                if (topicRepository.compareOffsets(cursor.getOffset(), currentOffset) > 0) {
                    zkHolder.get().setData().forPath(offsetPath, cursor.getOffset().getBytes(CHARSET));
                    return true;
                } else {
                    return false;
                }
            }, zkLockFactory.createLock(offsetPath));
        } catch (final IllegalArgumentException e) {
            throw new InvalidCursorException(CursorError.INVALID_FORMAT, cursor);
        } catch (Exception e) {
            throw new ServiceUnavailableException("Error communicating with zookeeper", e);
        }
    }

}
