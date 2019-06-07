package org.zalando.nakadi.repository.zookeeper;

import java.io.Closeable;

public interface CoordinationService extends Closeable {

    void lockAcquire(String path, long timeoutMs) throws RuntimeException;

    void lockRelease(String path) throws RuntimeException;
}
