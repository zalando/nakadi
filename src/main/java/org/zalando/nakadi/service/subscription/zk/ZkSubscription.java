package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;

import java.io.Closeable;

public interface ZkSubscription<T> extends Closeable {

    T getData() throws NakadiRuntimeException;

    @Override
    void close();
}
