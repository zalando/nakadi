package org.zalando.nakadi.cache;

import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZookeeperNodeInvalidatorTest extends BaseAT {
    private static final ZooKeeperHolder CURATOR = ZookeeperTestUtils.createZkHolder(ZOOKEEPER_URL);

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
            Thread.sleep(100);

            // Notify about updates several times and ensure that all caches are updated.
            for (int idx = 0; idx < 10; ++idx) {
                invalidators.get(idx % invalidators.size()).notifyUpdate();
                Thread.sleep(100);

                for (final Cache c : caches) {
                    Mockito.verify(c, Mockito.times(idx + 2)).refresh();
                }
            }

        } finally {
            invalidators.forEach(ZookeeperNodeInvalidator::stop);
        }
    }

    @Test(timeout = 1000)
    public void cachePeriodicUpdateTriggered() throws InterruptedException {
        final Cache cache = Mockito.mock(Cache.class);

        final ZookeeperNodeInvalidator invalidator =
                new ZookeeperNodeInvalidator(cache, CURATOR, "/xxx/yyy1", 100);
        invalidator.start();
        try {
            Thread.sleep(500);
            Mockito.verify(cache, Mockito.atLeast(4)).refresh();
            Mockito.verify(cache, Mockito.atMost(6)).refresh();
        } finally {
            invalidator.stop();
        }
    }

}