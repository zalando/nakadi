package org.zalando.nakadi.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

public class SimpleCache
        <T, Versioned extends VersionedEntity<Version>, Version extends Comparable>
        implements Cache<T> {

    private final CacheDataProvider<Versioned, Version> dataProvider;
    private final Function<Versioned, T> rawDataConverter;

    private final Map<String, Versioned> versionedData = new HashMap<>();
    private final Object versionedDataLock = new Object();

    private final ConcurrentHashMap<String, T> activeData = new ConcurrentHashMap<>();
    private final List<Consumer<String>> invalidationListeners = new CopyOnWriteArrayList<>();

    public SimpleCache(
            final CacheDataProvider<Versioned, Version> dataProvider,
            final Function<Versioned, T> rawDataConverter) {
        this.dataProvider = dataProvider;
        this.rawDataConverter = rawDataConverter;
    }

    private T loadItemLocked(final String key) {
        // this operation is relatively long, therefore we are making it concurrent
        final Versioned value = dataProvider.load(key);
        if (null == value) {
            return null;
        }
        synchronized (versionedDataLock) {
            versionedData.put(key, value);
        }
        // Here we also can spend some time
        return rawDataConverter.apply(value);
    }

    @Override
    public T get(final String key) {
        return activeData.computeIfAbsent(key, this::loadItemLocked);
    }

    @Override
    public void invalidate(final String key) {
        synchronized (versionedDataLock) {
            versionedData.remove(key);
        }
        activeData.remove(key);
        invalidationListeners.forEach(l -> l.accept(key));
    }

    @Override
    public synchronized void addInvalidationListener(final Consumer<String> listener) {
        invalidationListeners.add(listener);
    }

    @Override
    public void removeInvalidationListener(final Consumer<String> listener) {
        invalidationListeners.remove(listener);
    }

    @Override
    public synchronized void refresh() { // synchronized on object to avoid multiple sy
        // someone detected, that cache should be refreshed. The event of refresh is taking place after
        // the actual data change, so it is safe to work on a data that is a bit older, and we do not care about
        final Set<Versioned> currentKeys;
        synchronized (versionedDataLock) {
            currentKeys = new HashSet<>(versionedData.values()); // We are intentionally creating copy of values
        }

        final CacheChange fullChangeList = this.dataProvider.getFullChangeList(currentKeys);
        // There are 2 strategies on this kind of updates.
        // 1 - Load data in batch mode and update values
        // 2 - Just remove data from cache.
        // As massive deletion of event types is actually rare operation, we will use second approach (it's simpler)

        // order of removal is important, btw.
        synchronized (versionedDataLock) {
            fullChangeList.getDeletedKeys().forEach(versionedData::remove);
            fullChangeList.getModifiedKeys().forEach(versionedData::remove);
        }
        // Once registered data was removed - we can remove it from cache.
        // It's fine if versionedData will contain values, while activeList - not.
        fullChangeList.getDeletedKeys().forEach(activeData::remove);
        fullChangeList.getModifiedKeys().forEach(activeData::remove);

        invalidationListeners.forEach(l -> fullChangeList.getDeletedKeys().forEach(l));
        invalidationListeners.forEach(l -> fullChangeList.getModifiedKeys().forEach(l));
    }

}
