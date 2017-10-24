package org.zalando.nakadi.service.subscription.zk;

import java.io.Closeable;

public interface ZkSubscr<T> extends Closeable {

    T getData() throws Exception;

    @Override
    void close();
}
