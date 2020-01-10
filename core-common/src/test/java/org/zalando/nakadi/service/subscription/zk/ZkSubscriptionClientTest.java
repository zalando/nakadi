package org.zalando.nakadi.service.subscription.zk;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
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

    @Test
    public void testSerializationDeserialization() throws IOException {
        final ZkSubscriptionClient.Topology first = new ZkSubscriptionClient.Topology(
                new Partition[]{new Partition("1", "2", "3", "4", Partition.State.ASSIGNED)},
                "123",
                456);

        final String serialized = TestUtils.OBJECT_MAPPER.writer().writeValueAsString(first);

        final ZkSubscriptionClient.Topology second = TestUtils.OBJECT_MAPPER.readValue(
                serialized, ZkSubscriptionClient.Topology.class);

        Assert.assertEquals(first, second);
    }

}
