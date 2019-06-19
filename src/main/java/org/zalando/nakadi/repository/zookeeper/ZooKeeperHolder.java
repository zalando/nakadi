package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.zalando.nakadi.config.NakadiSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ZooKeeperHolder {

    private static final int EXHIBITOR_RETRY_TIME = 1000;
    private static final int EXHIBITOR_RETRY_MAX = 3;
    private static final int EXHIBITOR_POLLING_MS = 300000;

    private final String zookeeperBrokers;
    private final String zookeeperKafkaNamespace;
    private final String exhibitorAddresses;
    private final Integer exhibitorPort;
    private final Integer sessionTimeoutMs;
    private final Integer connectionTimeoutMs;
    private final long maxCommitTimeoutMs;

    private CuratorFramework zooKeeper;
    private CuratorFramework subscriptionCurator;

    public ZooKeeperHolder(final String zookeeperBrokers,
                           final String zookeeperKafkaNamespace,
                           final String exhibitorAddresses,
                           final Integer exhibitorPort,
                           final Integer sessionTimeoutMs,
                           final Integer connectionTimeoutMs,
                           final NakadiSettings nakadiSettings) throws Exception {
        this.zookeeperBrokers = zookeeperBrokers;
        this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        this.exhibitorAddresses = exhibitorAddresses;
        this.exhibitorPort = exhibitorPort;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.maxCommitTimeoutMs = TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout());

        initExhibitor();
    }

    private void initExhibitor() throws Exception {
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX);
        final EnsembleProvider ensembleProvider;
        if (exhibitorAddresses != null) {
            final Collection<String> exhibitorHosts = Arrays.asList(exhibitorAddresses.split("\\s*,\\s*"));
            final Exhibitors exhibitors = new Exhibitors(exhibitorHosts, exhibitorPort,
                    () -> zookeeperBrokers + zookeeperKafkaNamespace);
            final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
            ensembleProvider = new ExhibitorEnsembleProvider(exhibitors,
                    exhibitorRestClient, "/exhibitor/v1/cluster/list", EXHIBITOR_POLLING_MS, retryPolicy);
            ((ExhibitorEnsembleProvider) ensembleProvider).pollForInitialEnsemble();
        } else {
            ensembleProvider = new FixedEnsembleProvider(zookeeperBrokers + zookeeperKafkaNamespace);
        }
        zooKeeper = CuratorFrameworkFactory.builder()
                .ensembleProvider(ensembleProvider)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        zooKeeper.start();

        subscriptionCurator = CuratorFrameworkFactory.builder()
                .ensembleProvider(ensembleProvider)
                .retryPolicy(retryPolicy)
                // max commit timeout is not higher than 60 seconds, it is safe to cast yo integer
                .sessionTimeoutMs((int) maxCommitTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        subscriptionCurator.start();
    }

    public CuratorFramework get() {
        return zooKeeper;
    }

    public CuratorFramework getSubscriptionCurator(final long sessionTimeoutMs) {
        // most of the clients use default max timeout, subscriptionCurator client saves zookeeper resource
        if (sessionTimeoutMs == maxCommitTimeoutMs) {
            return subscriptionCurator;
        }

        final CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zooKeeper.getZookeeperClient().getCurrentConnectionString())
                .retryPolicy(new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX))
                // max commit timeout is not higher than 60 seconds, it is safe to cast yo integer
                .sessionTimeoutMs((int) sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        curatorFramework.start();
        return curatorFramework;
    }

    private class ExhibitorEnsembleProvider extends org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider {

        ExhibitorEnsembleProvider(final Exhibitors exhibitors, final ExhibitorRestClient restClient,
                                  final String restUriPath, final int pollingMs, final RetryPolicy retryPolicy) {
            super(exhibitors, restClient, restUriPath, pollingMs, retryPolicy);
        }

        @Override
        public String getConnectionString() {
            return super.getConnectionString() + zookeeperKafkaNamespace;
        }
    }
}
