package org.zalando.nakadi.cache;

import javax.annotation.Nullable;
import java.util.function.Consumer;

public interface Cache<T> {
    /**
     * Returns value associated with a key. If value is not found, returns null.
     *
     * @param key Key to get value for
     * @return value associated with a key or null, if nothing was found.
     */
    @Nullable
    T get(String key);

    /**
     * Sometimes it is important to ensure that cache will be providing refreshed version of value.
     * This method ensures that value will be refreshed.
     *
     * @param key key to invalidate
     */
    void invalidate(String key);

    /**
     * Adds listener for key invalidation. It is guaranteed, that any event type that was loaded into cache
     * previously and changed afterwards or cleared up through {@code invalidate} call will go through this method.
     *
     * @param listener Listener to add.
     */
    void addInvalidationListener(Consumer<String> listener);

    /**
     * Removes invalidation listener
     *
     * @param listener Listener to remove
     */
    void removeInvalidationListener(Consumer<String> listener);

    /**
     * Method is used to synchronize cache state with data source state. Basically it works in a way that
     * cache sends to datasource information about values that are stored in cache, cache data source in its turn
     * identifies changes occurred and sends keys that were deleted/modified, informing cache that this data should
     * be reloaded (invalidated)
     */
    void refresh();
}
