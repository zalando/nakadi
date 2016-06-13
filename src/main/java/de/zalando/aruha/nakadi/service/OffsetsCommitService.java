package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import java.nio.charset.Charset;

import static de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperUtils.runLocked;
import static java.text.MessageFormat.format;

public class OffsetsCommitService {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final CuratorFramework curatorFramework;
    private final TopicRepository topicRepository;

    public OffsetsCommitService(final CuratorFramework curatorFramework, final TopicRepository topicRepository) {
        this.curatorFramework = curatorFramework;
        this.topicRepository = topicRepository;
    }

    public boolean commitCursor(final String subscriptionId, final String eventType, final Cursor cursor)
            throws ServiceUnavailableException {

        final String offsetPath = format("/nakadi/subscriptions/{0}/topics/{1}/partitions/{2}/offsets",
                subscriptionId, eventType, cursor.getPartition());

        final InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(curatorFramework, offsetPath);

        try {
            return runLocked(() -> {
                final String currentOffset = new String(curatorFramework.getData().forPath(offsetPath), CHARSET);
                if (topicRepository.compareOffsets(cursor.getOffset(), currentOffset) > 0) {
                    curatorFramework.setData().forPath(offsetPath, cursor.getOffset().getBytes(CHARSET));
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
