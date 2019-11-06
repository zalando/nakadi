package org.zalando.nakadi.cache;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZookeeperNodeInvalidatorTest extends BaseAT {
    private static final ZooKeeperHolder CURATOR = ZookeeperTestUtils.createZkHolder(ZOOKEEPER_URL);

    private static void waitForNCachesRefreshes(
            final int expectedRefreshes, final boolean strict, final Collection<Cache> caches) {
        TestUtils.waitFor(() -> {
            for (final Cache c : caches) {
                if (strict) {
                    Mockito.verify(c, Mockito.times(expectedRefreshes)).refresh();
                } else {
                    Mockito.verify(c, Mockito.atLeast(expectedRefreshes)).refresh();
                }
            }
        }, TimeUnit.MINUTES.toMillis(10), 50); // Every 50 ms to check.
    }

    @Test(timeout = 2000)
    public void cacheUpdateTriggeredOnNodeChange() throws InterruptedException {
        final int parralelism = 10;
        final List<Cache> caches = new ArrayList<>();
        for (int i = 0; i < parralelism; ++i) {
            caches.add(Mockito.mock(Cache.class));
        }

        final List<ZookeeperNodeInvalidator> invalidators = caches
                .stream()
                .map(v -> new ZookeeperNodeInvalidator(v, CURATOR, "/xxx/yyy", TimeUnit.MINUTES.toMillis(10)))
                .collect(Collectors.toList());

        invalidators.forEach(ZookeeperNodeInvalidator::start);
        try {
            // Wait 100 ms for caches to start
            waitForNCachesRefreshes(1, true, caches);

            // Notify about updates several times and ensure that all caches are updated.
            for (int idx = 0; idx < 5; ++idx) {
                invalidators.get(idx % invalidators.size()).notifyUpdate();

                waitForNCachesRefreshes(idx + 2, true, caches);
            }

        } finally {
            invalidators.forEach(ZookeeperNodeInvalidator::stop);
        }
    }

    @Test(timeout = 1000)
    public void cachePeriodicUpdateTriggered() throws InterruptedException {
        final Cache cache = Mockito.mock(Cache.class);

        final long forceRefreshMs = 100;
        final ZookeeperNodeInvalidator invalidator =
                new ZookeeperNodeInvalidator(cache, CURATOR, "/xxx/yyy1", forceRefreshMs);
        invalidator.start();
        try {
            final long startedAt = System.currentTimeMillis();
            waitForNCachesRefreshes(6, false, Collections.singleton(cache));
            final long allRefreshesFinished = System.currentTimeMillis();

            // Wea are verifying that refreshes are triggered not one after another, but with proper delay
            Assert.assertTrue((allRefreshesFinished - startedAt) >= (4 * forceRefreshMs));
        } finally {
            invalidator.stop();
        }
    }
}
