package org.zalando.nakadi.cache;

import java.util.Collection;

/**
 * Defines interface for cache data provider.
 *
 * @param <T> the type of cached entity
 * @param <V> Type of version used for entities
 */
public interface CacheDataProvider<T extends VersionedEntity<V>, V extends Comparable> {
    /**
     * Loads value for cache. In case if value is not found method should return null. Several calls of this method may
     * run simultaneously
     * @param key key to get data for.
     * @return Returns value to store for key.
     */
    T load(String key);

    /**
     * Returns changes to the cache that are currently in database. The calls to this method are serialized.
     * @param snapshot current cache data.
     * @return Changed values (updated/deleted). If value is not changed (version is the same), then it should not
     * present in changed values
     */
    CacheChange getFullChangeList(Collection<T> snapshot);
}
