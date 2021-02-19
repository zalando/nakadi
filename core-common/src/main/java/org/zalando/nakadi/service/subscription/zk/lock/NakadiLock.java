package org.zalando.nakadi.service.subscription.zk.lock;

/**
 * Interface to provide distributed locking
 * between Nakadi instances
 */
public interface NakadiLock {

    /**
     * Corresponding unlock method should be called
     * in case lock was acquired, which is
     * indicated by return true boolean flag.
     */
    boolean lock();

    /**
     * Unlock acquired lock in the lock method.
     */
    void unlock();
}
