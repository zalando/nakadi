package org.zalando.nakadi.repository.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;

import java.io.Closeable;

/**
 * Non thread safe implementation of lock service backed by Apache Zookeeper.
 */
public class ZookeeperLock {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLock.class);
    private static final String NAKADI_LOCKS = "/nakadi/locks";
    private static final int RETRY_DELAY_MS = 30;
    private static final int RETRY_MAX_COUNT = 10;
    private final ZooKeeper zookeeper;

    public ZookeeperLock(final ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
        try {
            zookeeper.create(NAKADI_LOCKS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (final KeeperException e) {
            LOG.trace("Error creating locks node", e);
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while creating locks node", e);
        }
    }

    public Closeable tryLock(final String lockNode, final long timeoutMs) throws RuntimeException {
        final String lockPath = NAKADI_LOCKS + "/" + lockNode;
        final long finishAt = System.currentTimeMillis() + timeoutMs;
        while (finishAt > System.currentTimeMillis()) {
            try {
                zookeeper.create(lockPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return () -> lockRelease(lockPath);
            } catch (final KeeperException.SessionExpiredException e) {
                close();
                throw new RuntimeException("Failed to acquire lock", e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                close();
                throw new RuntimeException("Failed to acquire lock", e);
            } catch (final KeeperException e) {
                // lock is taken or network problem, retrying
                LOG.warn("Failed to acquire lock for {}, retrying", lockPath, e);
            }

            retryDelay();
        }

        close();
        throw new RuntimeException("Timeout reached acquiring lock");
    }

    private void lockRelease(final String lockPath) throws RuntimeException {
        int requestCounter = 0;
        while (requestCounter < RETRY_MAX_COUNT) {
            requestCounter++;
            try {
                zookeeper.delete(lockPath, -1);
                close();
                return;
            } catch (final KeeperException.SessionExpiredException | KeeperException.NoNodeException e) {
                close();
                throw new RuntimeException("Failed to release lock", e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                close();
                throw new RuntimeException("Interrupted while acquiring lock", e);
            } catch (final KeeperException e) {
                // connection loss, retrying
                LOG.warn("Failed to release lock for {}, retrying", lockPath, e);
            }

            retryDelay();
        }
        LOG.error("Failed to release lock for {}", lockPath);
        close();
    }

    private void close() throws NakadiRuntimeException {
        try {
            zookeeper.close();
        } catch (final InterruptedException e) {
            LOG.error("Interrupted while closing zookeeper client", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while closing client", e);
        }
    }

    private void retryDelay() {
        try {
            Thread.sleep(RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while sleeping in retry", ie);
        }
    }

}
