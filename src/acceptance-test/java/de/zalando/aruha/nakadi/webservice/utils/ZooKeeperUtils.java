package de.zalando.aruha.nakadi.webservice.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class ZooKeeperUtils {

    private ZooKeeperUtils() {
    }

    public static CuratorFramework createCurator(final String zkUrl) {
        final CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new RetryNTimes(5, 500))
                .build();
        curator.start();
        return curator;
    }
}
