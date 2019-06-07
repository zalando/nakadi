package org.zalando.nakadi.repository.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Non thread safe implementation of coordination service backed by Apache Zookeeper.
 */
public class ZookeeperProxy implements CoordinationService {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperProxy.class);
    private static final int RETRY_DELAY_MS = 250;
    private static final int RETRY_MAX_COUNT = 10;
    private final ZooKeeper zookeeper;

    public ZookeeperProxy(final ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
        try {
            zookeeper.create("/nakadi/locks", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (final KeeperException e) {
            LOG.trace("Error creating locks", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockAcquire(final String path, final long timeoutMs) throws RuntimeException {
        int requestCounter = 0;
        final byte[] sessionId = String.valueOf(zookeeper.getSessionId()).getBytes();
        final long finishAt = System.currentTimeMillis() + timeoutMs;
        while (finishAt > System.currentTimeMillis()) {
            requestCounter++;
            try {
                zookeeper.create(path, sessionId, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return;
            } catch (final Exception e) {
                if (e instanceof KeeperException) {
                    LOG.error("Error while creating lock node, will check session id", e);
                    try {
                        final byte[] storedSessionId = zookeeper.getData(path, false, null);
                        if (Arrays.equals(sessionId, storedSessionId)) {
                            return;
                        }
                    } catch (final Exception e2) {
                        processException(e2);
                    }
                }
                processException(e);
            }

            retryDelay(requestCounter);
        }

        throw new RuntimeException("Timeout reached acquiring lock");
    }

    @Override
    public void lockRelease(final String path) throws RuntimeException {
        int requestCounter = 0;
        final byte[] sessionId = String.valueOf(zookeeper.getSessionId()).getBytes();
        while (requestCounter < RETRY_MAX_COUNT) {
            requestCounter++;
            try {
                final byte[] storedSessionId = zookeeper.getData(path, false, null);
                if (Arrays.equals(sessionId, storedSessionId)) {
                    try {
                        zookeeper.delete(path, -1);
                        return;
                    } catch (final Exception e) {
                        processException(e);
                    }
                } else {
                    throw new RuntimeException("Can not release the lock which was not acquired");
                }
            } catch (final Exception e) {
                processException(e);
            }

            retryDelay(requestCounter);
        }
        LOG.error("Failed to release lock for {}", path);
    }

    @Override
    public void close() {
        try {
            zookeeper.close();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while closing zookeeper client", e);
        }
    }


    private void retryDelay(final int requestCounter) {
        try {
            Thread.sleep(requestCounter * RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void processException(final Exception e) {
        LOG.error("Failed working with lock", e);
        if (e instanceof KeeperException.SessionExpiredException) {
            throw new RuntimeException("Failed to acquire lock", e);
        }

        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

}
