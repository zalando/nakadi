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
    private CuratorFramework zooKeeper;

    public ZooKeeperHolder(final String zookeeperBrokers,
                           final String zookeeperKafkaNamespace,
                           final String exhibitorAddresses,
                           final Integer exhibitorPort) throws Exception {
        this.zookeeperBrokers = zookeeperBrokers;
        this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        this.exhibitorAddresses = exhibitorAddresses;
        this.exhibitorPort = exhibitorPort;
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
                .build();
        zooKeeper.start();
    }

    public CuratorFramework get() {
        return zooKeeper;
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
