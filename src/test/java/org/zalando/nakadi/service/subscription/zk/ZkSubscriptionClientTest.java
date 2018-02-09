package org.zalando.nakadi.service.subscription.zk;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZkSubscriptionClientTest {

    @Test
    public void testHashCalculationOrder() {
        Assert.assertEquals(
                ZkSubscriptionClient.Topology.calculateSessionsHash(Stream.of("1", "2").collect(Collectors.toList())),
                ZkSubscriptionClient.Topology.calculateSessionsHash(Stream.of("2", "1").collect(Collectors.toList()))
        );
    }

    @Test
    public void testHashCalculationDifferent() {
        Assert.assertNotEquals(
                ZkSubscriptionClient.Topology.calculateSessionsHash(Stream.of("1", "3").collect(Collectors.toList())),
                ZkSubscriptionClient.Topology.calculateSessionsHash(Stream.of("2", "1").collect(Collectors.toList()))
        );
    }

}