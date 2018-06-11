package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.NakadiWrapperException;

import java.io.Closeable;

public interface ZkSubscription<T> extends Closeable {

    T getData() throws NakadiWrapperException;

    @Override
    void close();
}
