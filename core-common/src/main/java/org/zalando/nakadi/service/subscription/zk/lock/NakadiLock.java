package org.zalando.nakadi.service.subscription.zk.lock;

public interface NakadiLock {

    boolean lock();

    void unlock();
}
