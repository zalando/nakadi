package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class CuratorFrameworkRotator {

    private static final Logger LOG =
            LoggerFactory.getLogger(CuratorFrameworkRotator.class);

    private final Supplier<CuratorFramework> curatorSupplier;
    private final long curatorMaxLifetimeMs;
    private final ReadWriteLock lock;
    private ScheduledExecutorService executor;

    private final CuratorUsage activeCurator;
    private final CuratorUsage retiringCurator;

    private long activeCuratorRotatedAt;

    public CuratorFrameworkRotator(
            final Supplier<CuratorFramework> curatorSupplier,
            final long curatorMaxLifetimeMs,
            final long curatorRotationCheckMs) {
        this.curatorSupplier = curatorSupplier;
        this.curatorMaxLifetimeMs = curatorMaxLifetimeMs;
        this.lock = new ReentrantReadWriteLock();
        this.activeCurator = new CuratorUsage
                (curatorSupplier.get(),
                        new AtomicLong());
        this.retiringCurator = new CuratorUsage(null, null);
        this.activeCuratorRotatedAt = System.currentTimeMillis();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(this::scheduleRotationCheck,
                curatorRotationCheckMs, curatorRotationCheckMs, TimeUnit.MILLISECONDS);
    }

    public CuratorFramework takeCuratorFramework() {
        lock.readLock().lock();
        try {
            activeCurator.counter.incrementAndGet();
            return activeCurator.curator;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void returnCuratorFramework(
            final CuratorFramework curatorFramework)
            throws IllegalStateException,
            IllegalArgumentException {

        if (curatorFramework == null) {
            throw new IllegalArgumentException(
                    "CuratorFramework can not be null");
        }

        lock.readLock().lock();
        try {
            if (curatorFramework == activeCurator.curator) {
                activeCurator.counter.decrementAndGet();
            } else if (curatorFramework == retiringCurator.curator) {
                retiringCurator.counter.decrementAndGet();
            } else {
                throw new IllegalStateException(
                        "unexpected returned curator framework");
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void scheduleRotationCheck() {
        try {
            if (System.currentTimeMillis() >
                    activeCuratorRotatedAt + curatorMaxLifetimeMs) {
                checkAndRotateCurator();
            }

            checkAndRecycleCurator();
        } catch (final RuntimeException re) {
            LOG.error("failed to run rotation task, will try again", re);
        }
    }

    private void checkAndRotateCurator() {
        if (retiringCurator.curator == null) {
            final CuratorFramework tmpCurator = activeCurator.curator;
            final AtomicLong tmpCounter = activeCurator.counter;
            lock.writeLock().lock();
            try {
                activeCurator.curator = curatorSupplier.get();
                activeCurator.counter = new AtomicLong();
                retiringCurator.curator = tmpCurator;
                retiringCurator.counter = tmpCounter;
            } finally {
                lock.writeLock().unlock();
            }
            activeCuratorRotatedAt = System.currentTimeMillis();
            LOG.info("curator client rotated, current usage {}",
                    retiringCurator.counter);
        } else {
            LOG.debug("waiting for retired client recycling, " +
                    "current usage {}", retiringCurator.counter);
        }
    }

    private void checkAndRecycleCurator() {
        if (retiringCurator.counter != null &&
                retiringCurator.counter.get() == 0) {
            lock.writeLock().lock();
            try {
                retiringCurator.curator.close();
                retiringCurator.curator = null;
                retiringCurator.counter = null;
                LOG.info("curator client recycled");
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private class CuratorUsage {
        private CuratorFramework curator;
        private AtomicLong counter;

        private CuratorUsage(
                final CuratorFramework curator,
                final AtomicLong counter) {
            this.curator = curator;
            this.counter = counter;
        }
    }
}
