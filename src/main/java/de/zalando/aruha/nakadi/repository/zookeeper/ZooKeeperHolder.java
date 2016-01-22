package de.zalando.aruha.nakadi.repository.zookeeper;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZooKeeperHolder {

    private final String zookeeperBrokers;

    private final String zookeeperKafkaNamespace;

    private final String exhibitorAddresses;

    private final Integer exhibitorPort;

    private CuratorFramework zooKeeper;

    public ZooKeeperHolder(String zookeeperBrokers, String zookeeperKafkaNamespace, String exhibitorAddresses, Integer exhibitorPort) {
        this.zookeeperBrokers = zookeeperBrokers;
        this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        this.exhibitorAddresses = exhibitorAddresses;
        this.exhibitorPort = exhibitorPort;
    }

    class ExhibitorEnsembleProvider extends org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider {

        public ExhibitorEnsembleProvider(Exhibitors exhibitors, ExhibitorRestClient restClient, String restUriPath,
                int pollingMs, RetryPolicy retryPolicy) {
            super(exhibitors, restClient, restUriPath, pollingMs, retryPolicy);
        }

        @Override
        public String getConnectionString() {
            return super.getConnectionString() + zookeeperKafkaNamespace;
        }
    }

    @PostConstruct
    public void init() throws Exception {
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        EnsembleProvider ensembleProvider;
        if (exhibitorAddresses != null) {
            final Collection<String> exhibitorHosts = Arrays.asList(exhibitorAddresses.split("\\s*,\\s*"));
            final Exhibitors exhibitors = new Exhibitors(exhibitorHosts, exhibitorPort,
                    () -> zookeeperBrokers + zookeeperKafkaNamespace);
            final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
            ensembleProvider = new ExhibitorEnsembleProvider(exhibitors,
                    exhibitorRestClient, "/exhibitor/v1/cluster/list", 300000, retryPolicy);
            ((ExhibitorEnsembleProvider)ensembleProvider).pollForInitialEnsemble();
        } else {
            ensembleProvider = new FixedEnsembleProvider(zookeeperBrokers + zookeeperKafkaNamespace);
        }
        zooKeeper = CuratorFrameworkFactory.builder().ensembleProvider(ensembleProvider).retryPolicy(retryPolicy).build();
        zooKeeper.start();
    }

    public CuratorFramework get() {
        return zooKeeper;
    }
}
