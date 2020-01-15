package org.zalando.nakadi.domain.storage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class ZookeeperConnectionTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{
                        "exhibitor://localhost:8181/path",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.EXHIBITOR,
                                Collections.singletonList(new AddressPort("localhost", 8181)),
                                "/path")
                }, new Object[]{
                        "zookeeper://localhost:8181/path",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.ZOOKEEPER,
                                Collections.singletonList(new AddressPort("localhost", 8181)),
                                "/path")
                }, new Object[]{
                        "zookeeper://localhost:8181",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.ZOOKEEPER,
                                Collections.singletonList(new AddressPort("localhost", 8181)),
                                null)
                }, new Object[]{
                        "zookeeper://localhost:8181,localhost2:2181",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.ZOOKEEPER,
                                Arrays.asList(
                                        new AddressPort("localhost", 8181),
                                        new AddressPort("localhost2", 2181)),
                                null)
                }, new Object[]{
                        "zookeeper://localhost:8181,localhost2:2181/testpath",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.ZOOKEEPER,
                                Arrays.asList(
                                        new AddressPort("localhost", 8181),
                                        new AddressPort("localhost2", 2181)),
                                "/testpath")
                }, new Object[]{
                        "zookeeper://127.0.0.1:8181,127.0.0.2:2181/test/path",
                        new ZookeeperConnection(
                                ZookeeperConnectionType.ZOOKEEPER,
                                Arrays.asList(
                                        new AddressPort("127.0.0.1", 8181),
                                        new AddressPort("127.0.0.2", 2181)),
                                "/test/path")
                }
        );
    }

    private final String value;
    private final ZookeeperConnection expectedConnection;

    public ZookeeperConnectionTest(final String value, final ZookeeperConnection expectedConnection) {
        this.value = value;
        this.expectedConnection = expectedConnection;
    }

    @Test
    public void test() {
        Assert.assertEquals(
                expectedConnection,
                ZookeeperConnection.valueOf(value)
        );
    }
}
