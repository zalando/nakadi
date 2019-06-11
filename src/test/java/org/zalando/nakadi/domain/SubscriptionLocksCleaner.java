package org.zalando.nakadi.domain;

import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * If the amount of children in /nakadi/locks is so high that it can't be fetched by zookeeper client - then
 * this class should be run with VM parameter -Djute.maxbuffer=BUFFER_SIZE_BYTES where BUFFER_SIZE_BYTES should
 * be the value that will fit the list of nodes, for example 41943040 (40 MB)
 */
public class SubscriptionLocksCleaner {

    public static void main(String args[]) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 30000, event -> {
        });

        final String contextPath = "/staging";

        final List<String> locks = zk.getChildren(contextPath + "/nakadi/locks", false);

        System.out.println(locks.size() + "Waiting 70 seconds before fetching list of subscriptions...");
        Thread.sleep(70000); // we need to wait here to avoid deletion of locks for just started subscriptions

        final Set<String> subscriptions = new HashSet<>(zk.getChildren(contextPath + "/nakadi/subscriptions", false));
        System.out.println(locks.size() + " locks, " + subscriptions.size() + " subscriptions");

        int notExists = 0;
        for (int i = 0; i < locks.size(); i++) {
            String lockName = locks.get(i);
            final String subscriptionId = lockName.substring(13);

            if (!subscriptions.contains(subscriptionId)) {
                ZKUtil.deleteRecursive(zk, contextPath + "/nakadi/locks/" + lockName);
                System.out.println("Removed lock for " + subscriptionId + " as it doesn't exist");
                notExists++;
            }
            System.out.println(((i + 1) * 100 / locks.size()) + "%");
        }
        System.out.println("Exist: " + (locks.size() - notExists) + ", Removed: " + notExists);
    }

}
