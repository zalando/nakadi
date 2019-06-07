package org.zalando.nakadi.domain;

import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubscriptionLocksCleaner {

    public static void main(String args[]) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 30000, event -> {
        });

        final String contextPath = "/staging";

        final List<String> locks = zk.getChildren(contextPath + "/nakadi/locks", false);
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
