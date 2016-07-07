package de.zalando.aruha.nakadi.util;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class TimeBasedCacheTest {

    @Test
    public void valueMustBeCachedAndRemovedAfterTimeout() throws InterruptedException {
        final TimeBasedCache<String, String> cache = new TimeBasedCache<>(1000);

        final AtomicBoolean called = new AtomicBoolean(false);
        final String result = cache.getOrCalculate("test", (val) -> {
            called.set(true);
            return val;
        });
        final long start = System.currentTimeMillis();
        Assert.assertEquals("test", result);
        Assert.assertTrue(called.get());

        for (int i = 0; i < 5; ++i) {
            called.set(false);
            final String tmpResult = cache.getOrCalculate("test", (v) -> {
                called.set(true);
                return "sss";
            });
            Assert.assertEquals("test", tmpResult);
            Assert.assertFalse(called.get());
            Thread.sleep(100);
        }

        Thread.sleep(1000 - (System.currentTimeMillis() - start) + 1);

        final String finalResult = cache.getOrCalculate("test", (v) -> {
            called.set(true);
            return "response";
        });

        Assert.assertEquals("response", finalResult);
        Assert.assertTrue(called.get());
    }

}