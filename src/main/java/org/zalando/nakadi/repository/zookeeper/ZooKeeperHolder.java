package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.zalando.nakadi.domain.storage.AddressPort;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;

import java.util.stream.Collectors;

public class ZooKeeperHolder {

    private static final int EXHIBITOR_RETRY_TIME = 1000;
    private static final int EXHIBITOR_RETRY_MAX = 3;
    private static final int EXHIBITOR_POLLING_MS = 300000;

    private CuratorFramework zooKeeper;

    public ZooKeeperHolder(final ZookeeperConnection zookeeperConnection,
                           final Integer sessionTimeoutMs,
                           final Integer connectionTimeoutMs) throws Exception {
        final EnsembleProvider ensembleProvider = createEnsembleProvider(zookeeperConnection);

        zooKeeper = CuratorFrameworkFactory.builder()
                .ensembleProvider(ensembleProvider)
                .retryPolicy(new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        zooKeeper.start();
    }

    private EnsembleProvider createEnsembleProvider(final ZookeeperConnection conn) throws Exception {
        switch (conn.getType()) {
            case EXHIBITOR:
                final Exhibitors exhibitors = new Exhibitors(
                        conn.getAddresses().stream().map(AddressPort::getAddress).collect(Collectors.toList()),
                        conn.getAddresses().get(0).getPort(),
                        () -> {
                            throw new RuntimeException("There is no backup connection string (or it is wrong)");
                        });
                final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
                final ExhibitorEnsembleProvider result = new ExhibitorEnsembleProvider(
                        exhibitors,
                        exhibitorRestClient,
                        "/exhibitor/v1/cluster/list",
                        EXHIBITOR_POLLING_MS,
                        new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX)) {
                    @Override
                    public String getConnectionString() {
                        return super.getConnectionString() + conn.getPathPrepared();
                    }
                };
                result.pollForInitialEnsemble();
                return result;
            case ZOOKEEPER:
                final String address = conn.getAddresses().stream()
                        .map(AddressPort::asAddressPort)
                        .collect(Collectors.joining(","));
                return new FixedEnsembleProvider(address + conn.getPathPrepared());
            default:
                throw new RuntimeException("Connection type " + conn.getType() + " is not supported");
        }
    }

    public CuratorFramework get() {
        return zooKeeper;
    }
}
