package org.zalando.nakadi.cache;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class SimpleCacheTest {

    public static class Value implements VersionedEntity<String> {

        private final String key;
        private final String version;

        public Value(final String key, final String version) {
            this.key = key;
            this.version = version;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getVersion() {
            return version;
        }
    }

    @Test
    public void testGet() {
        final CacheDataProvider dataProvider = Mockito.mock(CacheDataProvider.class);
        final SimpleCache cache = new SimpleCache<>(dataProvider, t -> t);
        final Value existingItem = new Value("found", "1");

        Mockito.when(dataProvider.load("not-found")).thenReturn(null);
        Mockito.when(dataProvider.load("found")).thenReturn(existingItem);

        Assert.assertNull(cache.get("not-found"));
        Assert.assertEquals(existingItem, cache.get("found"));

        for (int i = 0; i < 10; ++i) {
            cache.get("found");
        }
        cache.get("not-found");

        Mockito.verify(dataProvider, Mockito.times(1)).load("found");
        Mockito.verify(dataProvider, Mockito.times(2)).load("not-found");
    }

    @Test
    public void testInvalidate() {
        final CacheDataProvider dataProvider = Mockito.mock(CacheDataProvider.class);
        final SimpleCache cache = new SimpleCache<>(dataProvider, t -> t);

        final Value existingItem1 = new Value("found", "1");
        final Value existingItem2 = new Value("found", "2");
        Mockito.when(dataProvider.load("found")).thenReturn(existingItem1, existingItem2);

        Assert.assertEquals(existingItem1, cache.get("found"));
        cache.invalidate("found");
        Assert.assertEquals(existingItem2, cache.get("found"));
    }

    @Test
    public void testRefresh() {
        final CacheDataProvider dataProvider = Mockito.mock(CacheDataProvider.class);
        final SimpleCache cache = new SimpleCache<>(dataProvider, t -> t);

        final Value item1v1 = new Value("key1", "1");
        final Value item1v2 = new Value("key1", "2");

        final Value item2v1 = new Value("key2", "1");
        final Value item2v2 = new Value("key2", "2");

        final Value item3v1 = new Value("key3", "1");

        Mockito.when(dataProvider.load("key1")).thenReturn(item1v1, item1v2);
        Mockito.when(dataProvider.load("key2")).thenReturn(item2v1, item2v2);
        Mockito.when(dataProvider.load("key3")).thenReturn(item3v1, null, null);

        // 1. Ensure data is loaded
        Assert.assertEquals(item1v1, cache.get("key1"));
        Assert.assertEquals(item2v1, cache.get("key2"));
        Assert.assertEquals(item3v1, cache.get("key3"));

        Mockito.verify(dataProvider, Mockito.times(1)).load("key1");
        Mockito.verify(dataProvider, Mockito.times(1)).load("key2");
        Mockito.verify(dataProvider, Mockito.times(1)).load("key3");

        // 2. In case of refresh, inform that item_1 has changed and item3 - deleted.
        final Set<Value> expectedCallParams = new HashSet<>();
        expectedCallParams.add(item1v1);
        expectedCallParams.add(item2v1);
        expectedCallParams.add(item3v1);

        // Now inform that key1 changed and key3 was removed
        Mockito.when(dataProvider.getFullChangeList(Mockito.eq(expectedCallParams)))
                .thenReturn(new CacheChange(Arrays.asList("key1"), Arrays.asList("key3")));
        cache.refresh();

        // Now expect that key3 is resulting in null, key1 in second version, key2 - still first version
        // (as it is not changed)
        Assert.assertEquals(item1v2, cache.get("key1"));
        Assert.assertEquals(item2v1, cache.get("key2"));
        Assert.assertNull(cache.get("key3"));

        Mockito.verify(dataProvider, Mockito.times(2)).load("key1");
        Mockito.verify(dataProvider, Mockito.times(1)).load("key2");
        Mockito.verify(dataProvider, Mockito.times(2)).load("key3");

        // And now call again, to check again
        Assert.assertEquals(item1v2, cache.get("key1"));
        Assert.assertEquals(item2v1, cache.get("key2"));
        Assert.assertNull(cache.get("key3"));

        Mockito.verify(dataProvider, Mockito.times(2)).load("key1");
        Mockito.verify(dataProvider, Mockito.times(1)).load("key2");
        Mockito.verify(dataProvider, Mockito.times(3)).load("key3");
    }

    @Test
    public void testInvalidationListeners() {
        // There are 2 ways to get to invalidation listener - through invalidate call and through cache refresh.
        // In order to achieve the result we will invalidate and record the invalidation listeners calls.

        final CacheDataProvider dataProvider = Mockito.mock(CacheDataProvider.class);
        final SimpleCache cache = new SimpleCache<>(dataProvider, t -> t);
        final List<String> recordedChanges = new ArrayList<>();
        cache.addInvalidationListener((Consumer<String>) recordedChanges::add);

        Mockito.when(dataProvider.load("key1")).thenReturn(new Value("key1", "1"));
        Mockito.when(dataProvider.load("key2")).thenReturn(new Value("key1", "1"));
        Mockito.when(dataProvider.load("key3")).thenReturn(new Value("key1", "1"));

        cache.get("key1");
        cache.get("key2");
        cache.get("key3");

        Assert.assertEquals(Collections.emptyList(), recordedChanges);
        cache.invalidate("key1");
        Assert.assertEquals(Arrays.asList("key1"), recordedChanges);
        cache.get("key1"); // Let's put it again to cache

        Mockito.when(dataProvider.getFullChangeList(Mockito.anySet())).thenReturn(
                new CacheChange(Arrays.asList("key3"), Arrays.asList("key1")));
        cache.refresh();
        Assert.assertEquals(Arrays.asList("key1", "key1", "key3"), recordedChanges);
    }
}