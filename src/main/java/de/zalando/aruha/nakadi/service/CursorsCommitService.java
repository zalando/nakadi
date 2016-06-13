package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import java.nio.charset.Charset;

import static de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;
import static java.text.MessageFormat.format;

public class CursorsCommitService {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final ZooKeeperHolder zkHolder;
    private final TopicRepository topicRepository;

    public CursorsCommitService(final ZooKeeperHolder zkHolder, final TopicRepository topicRepository) {
        this.zkHolder = zkHolder;
        this.topicRepository = topicRepository;
    }

    public boolean commitCursor(final String subscriptionId, final String eventType, final Cursor cursor)
            throws ServiceUnavailableException, NoSuchSubscriptionException {

        final String offsetPath = format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset",
                subscriptionId, eventType, cursor.getPartition());

        final InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(zkHolder.get(), offsetPath);

        try {
            return runLocked(() -> {
                final String currentOffset = new String(zkHolder.get().getData().forPath(offsetPath), CHARSET);
                if (topicRepository.compareOffsets(cursor.getOffset(), currentOffset) > 0) {
                    zkHolder.get().setData().forPath(offsetPath, cursor.getOffset().getBytes(CHARSET));
                    return true;
                } else {
                    return false;
                }
            }, lock);
        } catch (Exception e) {
            throw new ServiceUnavailableException("Error communicating with zookeeper", e);
        }
    }

}
