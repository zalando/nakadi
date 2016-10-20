package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.throttling.ThrottleResult;
import org.zalando.nakadi.throttling.ThrottlingService;
import org.zalando.nakadi.util.ZkConfigurationService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottleServiceTest {

    private final ZkConfigurationService zkConfigurationService = mock(ZkConfigurationService.class);

    @Before
    public void setUp() {
        when(zkConfigurationService.getLong("nakadi.throttling.bytesLimit")).thenReturn(10L);
        when(zkConfigurationService.getLong("nakadi.throttling.messagesLimit")).thenReturn(10L);
        when(zkConfigurationService.getLong("nakadi.throttling.batchesLimit")).thenReturn(3L);
    }

    @Test
    public void throttleByBytes() throws InterruptedException {
        ThrottlingService service = new ThrottlingService(1000, 1, zkConfigurationService);
        ThrottleResult result = service.mark("test1", "test1", 100, 1);
        Assert.assertTrue(result.isThrottled());

        ThrottleResult result2 = service.mark("test1", "test1", 10, 1);
        Assert.assertFalse(result2.isThrottled());

        Thread.sleep(2000);

        ThrottleResult result3 = service.mark("test1", "test1", 10, 1);
        Assert.assertFalse(result3.isThrottled());
    }

    @Test
    public void throttleByMessages() throws InterruptedException {
        ThrottlingService service = new ThrottlingService(1000, 1, zkConfigurationService);
        ThrottleResult result = service.mark("test2", "test2", 1, 100);
        Assert.assertTrue(result.isThrottled());

        ThrottleResult result2 = service.mark("test2", "test2", 1, 10);
        Assert.assertFalse(result2.isThrottled());

        Thread.sleep(2000);

        ThrottleResult result3 = service.mark("test2", "test2", 1, 10);
        Assert.assertFalse(result3.isThrottled());
    }

    @Test
    public void throttleByBatches() throws InterruptedException {
        ThrottlingService service = new ThrottlingService(1000, 1, zkConfigurationService);
        ThrottleResult result = service.mark("test3", "test3", 1, 1);
        Assert.assertFalse(result.isThrottled());

        ThrottleResult result1 = service.mark("test3", "test3", 1, 1);
        Assert.assertFalse(result1.isThrottled());

        ThrottleResult result2 = service.mark("test3", "test3", 1, 1);
        Assert.assertFalse(result2.isThrottled());

        ThrottleResult result3 = service.mark("test3", "test3", 1, 1);
        Assert.assertTrue(result3.isThrottled());

        Thread.sleep(2000);

        ThrottleResult result4 = service.mark("test3", "test3", 1, 1);
        Assert.assertFalse(result4.isThrottled());

    }
}
