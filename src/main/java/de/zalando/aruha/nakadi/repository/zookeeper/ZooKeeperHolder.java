package de.zalando.aruha.nakadi.repository.zookeeper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.exhibitor.Exhibitors.BackupConnectionStringProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class ZooKeeperHolder {

    @Autowired
    @Qualifier("zookeeperBrokers")
    private String zookeeperBrokers;

    @Autowired
    @Qualifier("zookeeperKafkaNamespace")
    private String zookeeperKafkaNamespace;

    @Autowired
    @Qualifier("exhibitorAddress")
    private String exhibitorAddress;

    @Autowired
    @Qualifier("exhibitorPort")
    private Integer exhibitorPort;

    private CuratorFramework zooKeeper;

    class ExhibitorEnsembleProvider extends org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider {

        private final String zookeeperKafkaNamespace;

        public ExhibitorEnsembleProvider(Exhibitors exhibitors, ExhibitorRestClient restClient, String restUriPath,
                int pollingMs, RetryPolicy retryPolicy, String zookeeperKafkaNamespace) {
            super(exhibitors, restClient, restUriPath, pollingMs, retryPolicy);
            this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        }

        @Override
        public String getConnectionString()
        {
            return super.getConnectionString() + zookeeperKafkaNamespace;
        }
    }

    @PostConstruct
    public void init() throws Exception {
        final Collection<String> exhibitorHosts = Arrays.asList(exhibitorAddress.split("\\s*,\\s*"));
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final Exhibitors exhibitors = new Exhibitors(exhibitorHosts, exhibitorPort, new BackupConnectionStringProvider() {
            @Override
            public String getBackupConnectionString() throws Exception {
                return zookeeperBrokers + zookeeperKafkaNamespace;
            }
        });
        final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
        final ExhibitorEnsembleProvider ensembleProvider = new ExhibitorEnsembleProvider(exhibitors, exhibitorRestClient,
                "/exhibitor/v1/cluster/list", 300000, retryPolicy, zookeeperKafkaNamespace);
        ensembleProvider.pollForInitialEnsemble();
        zooKeeper = CuratorFrameworkFactory.builder().ensembleProvider(ensembleProvider).retryPolicy(retryPolicy).build();
        zooKeeper.start();
    }

    public CuratorFramework get() throws IOException {
        return zooKeeper;
    }
}
