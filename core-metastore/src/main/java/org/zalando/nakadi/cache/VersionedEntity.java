package org.zalando.nakadi.cache;

public interface VersionedEntity<T extends Comparable> {
    String getKey();

    T getVersion();
}
