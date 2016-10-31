package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.throttling.ThrottleResult;
import org.zalando.nakadi.throttling.ThrottlingService;
import org.zalando.nakadi.util.ZkConfigurationService;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottleServiceTest {

    private final ZkConfigurationService zkConfigurationService = mock(ZkConfigurationService.class);

    @Before
    public void setUp() {
        when(zkConfigurationService.getLong(eq("nakadi.throttling.bytesLimit"), anyLong())).thenReturn(10L);
    }

    @Test
    public void throttleByBytes() throws InterruptedException {
        final ThrottlingService service = new ThrottlingService(1000, 1, zkConfigurationService);
        final ThrottleResult result = service.mark("test1", "test1", 100);
        Assert.assertTrue(result.isThrottled());

        final ThrottleResult result2 = service.mark("test1", "test1", 10);
        Assert.assertFalse(result2.isThrottled());

        Thread.sleep(2000);

        final ThrottleResult result3 = service.mark("test1", "test1", 10);
        Assert.assertFalse(result3.isThrottled());
    }
}
