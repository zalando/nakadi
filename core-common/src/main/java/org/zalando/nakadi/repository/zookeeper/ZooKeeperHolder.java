package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.storage.AddressPort;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.ZookeeperException;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZooKeeperHolder {

    private static final int EXHIBITOR_RETRY_TIME = 1000;
    private static final int EXHIBITOR_RETRY_MAX = 3;
    private static final int EXHIBITOR_POLLING_MS = 300000;

    private final Integer connectionTimeoutMs;
    private final long maxCommitTimeoutMs;
    private final ZookeeperConnection conn;

    private final CuratorFramework zooKeeper;
    private final CuratorFramework subscriptionCurator;

    public ZooKeeperHolder(final ZookeeperConnection conn,
                           final Integer sessionTimeoutMs,
                           final Integer connectionTimeoutMs,
                           final NakadiSettings nakadiSettings) throws Exception {
        this.conn = conn;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.maxCommitTimeoutMs = TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout());

        zooKeeper = createCuratorFramework(sessionTimeoutMs, connectionTimeoutMs);
        subscriptionCurator = createCuratorFramework((int) maxCommitTimeoutMs, connectionTimeoutMs);
    }

    public CuratorFramework get() {
        return zooKeeper;
    }

    public CloseableCuratorFramework getSubscriptionCurator(final long sessionTimeoutMs) throws ZookeeperException {
        // most of the clients use default max timeout, subscriptionCurator client saves zookeeper resource
        if (sessionTimeoutMs == maxCommitTimeoutMs) {
            return new StaticCuratorFramework(subscriptionCurator);
        }

        try {
            // max commit timeout is not higher than 60 seconds, it is safe to cast to integer
            return new DisposableCuratorFramework(createCuratorFramework((int) sessionTimeoutMs, connectionTimeoutMs));
        } catch (final Exception e) {
            throw new ZookeeperException("Failed to create curator framework", e);
        }
    }

    public abstract static class CloseableCuratorFramework implements Closeable {

        private final CuratorFramework curatorFramework;

        public CloseableCuratorFramework(final CuratorFramework curatorFramework) {
            this.curatorFramework = curatorFramework;
        }

        public CuratorFramework getCuratorFramework() {
            return curatorFramework;
        }
    }

    public static class StaticCuratorFramework extends CloseableCuratorFramework {

        public StaticCuratorFramework(final CuratorFramework curatorFramework) {
            super(curatorFramework);
        }

        @Override
        public void close() {
            // do not ever close this particular instance of curator
        }
    }

    public static class DisposableCuratorFramework extends CloseableCuratorFramework {

        public DisposableCuratorFramework(final CuratorFramework curatorFramework) {
            super(curatorFramework);
        }

        @Override
        public void close() {
            getCuratorFramework().close();
        }
    }

    private CuratorFramework createCuratorFramework(final int sessionTimeoutMs,
                                                    final int connectionTimeoutMs) throws Exception {
        final CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .ensembleProvider(createEnsembleProvider())
                .retryPolicy(new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        curatorFramework.start();
        return curatorFramework;
    }

    private EnsembleProvider createEnsembleProvider() throws Exception {
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
                final String addressesJoined = conn.getAddresses().stream()
                        .map(AddressPort::asAddressPort)
                        .collect(Collectors.joining(","));
                return new ChrootedFixedEnsembleProvider(addressesJoined, conn.getPathPrepared());
            default:
                throw new RuntimeException("Connection type " + conn.getType() + " is not supported");
        }
    }
}
