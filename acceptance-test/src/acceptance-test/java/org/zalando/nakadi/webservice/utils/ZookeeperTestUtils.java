package org.zalando.nakadi.webservice.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.concurrent.TimeUnit;

public class ZookeeperTestUtils {

    private ZookeeperTestUtils() {
    }

    public static CuratorFramework createCurator(final String zkUrl) {
        final CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new RetryNTimes(5, 500))
                .sessionTimeoutMs((int) TimeUnit.SECONDS.toMillis(10))
                .build();
        curator.start();
        return curator;
    }

    public static ZooKeeperHolder createZkHolder(final String zkUrl) {
        final CuratorFramework cf = createCurator(zkUrl);

        final ZooKeeperHolder mock = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(mock.get()).thenReturn(cf);
        return mock;
    }

}
