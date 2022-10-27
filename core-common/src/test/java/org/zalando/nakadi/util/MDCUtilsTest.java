package org.zalando.nakadi.util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.MDC;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MDCUtilsTest {

    @Test
    public void testEmptyEmptyContextTransfer() throws ExecutionException, InterruptedException, TimeoutException {
        // the method is checking that nothing goes down when there are no values set, and nulls are passed
        final MDCUtils.Context context = MDCUtils.getContext();
        try (MDCUtils.CloseableNoEx ignore = MDCUtils.withContext(context)) {
        }
    }

    @Test
    public void testNonEmptyEmptyContextTransfer() throws ExecutionException, InterruptedException, TimeoutException {
        // the method is checking that nothing goes down when there are no values set, and nulls are passed
        final MDCUtils.Context oldContext;
        try (MDCUtils.CloseableNoEx ignore1 = MDCUtils.withFlowId("1")) {
            oldContext = MDCUtils.getContext();
            Assert.assertEquals(MDCUtils.getFlowId(), "1");
        }
        Assert.assertNull(MDCUtils.getFlowId());
        try (MDCUtils.CloseableNoEx ignore2 = MDCUtils.withContext(oldContext)) {
            Assert.assertEquals(MDCUtils.getFlowId(), "1");
        }
        Assert.assertNull(MDCUtils.getFlowId());
    }

    @Test
    public void testEmptyNonEmptyContextTransfer() {
        final MDCUtils.Context oldContext = MDCUtils.getContext();
        try (MDCUtils.CloseableNoEx ignore1 = MDCUtils.withFlowId("1")) {
            Assert.assertEquals(MDCUtils.getFlowId(), "1");
            try (MDCUtils.CloseableNoEx ignore2 = MDCUtils.withContext(oldContext)) {
                Assert.assertEquals(MDCUtils.getFlowId(), null);
            }
            Assert.assertEquals(MDCUtils.getFlowId(), "1");
        }
        Assert.assertNull(MDCUtils.getFlowId());
    }

    @Test
    public void testNonEmptyNonEmptyContextTransfer() {
        final MDCUtils.Context oldContext;
        try (MDCUtils.CloseableNoEx ignore1 = MDCUtils.withSubscriptionId("1")) {
            oldContext = MDCUtils.getContext();
        }
        Assert.assertNull(MDC.get("subscriptionId"));
        try (MDCUtils.CloseableNoEx ignore2 = MDCUtils.withSubscriptionIdStreamId("2", "3")) {
            Assert.assertEquals(MDC.get("subscriptionId"), "2");
            Assert.assertEquals(MDC.get("streamId"), "3");
            try (MDCUtils.CloseableNoEx ignore3 = MDCUtils.withContext(oldContext)){
                Assert.assertEquals(MDC.get("subscriptionId"), "1");
                Assert.assertEquals(MDC.get("streamId"), null);
            }
            Assert.assertEquals(MDC.get("subscriptionId"), "2");
            Assert.assertEquals(MDC.get("streamId"), "3");
        }
        Assert.assertNull(MDC.get("subscriptionId"));
        Assert.assertNull(MDC.get("streamId"));
    }

}