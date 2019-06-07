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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;

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
    private final String connectionString;

    private CuratorFramework zooKeeper;

    public ZooKeeperHolder(final String zookeeperBrokers,
                           final String zookeeperKafkaNamespace,
                           final String exhibitorAddresses,
                           final Integer exhibitorPort,
                           final Integer sessionTimeoutMs,
                           final Integer connectionTimeoutMs) throws Exception {
        this.zookeeperBrokers = zookeeperBrokers;
        this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        this.exhibitorAddresses = exhibitorAddresses;
        this.exhibitorPort = exhibitorPort;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.connectionString =
                (exhibitorAddresses == null ? zookeeperBrokers : exhibitorAddresses) + zookeeperKafkaNamespace;

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
    }

    public CuratorFramework get() {
        return zooKeeper;
    }


    public Closeable newZookeeperLock(final String lockObject, final long timeoutMs) throws RuntimeException {
        try {
            final ZookeeperLock zookeeperLock = new ZookeeperLock(new ZooKeeper(connectionString,
                    sessionTimeoutMs, new NakadiZookeeperWatcher()));
            return zookeeperLock.tryLock(lockObject, timeoutMs);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to get zookeeper client", e);
        }
    }

    private static class NakadiZookeeperWatcher implements Watcher {
        private static final Logger LOG = LoggerFactory.getLogger(NakadiZookeeperWatcher.class);

        @Override
        public void process(final WatchedEvent event) {
            LOG.info("{}", event);
        }
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
