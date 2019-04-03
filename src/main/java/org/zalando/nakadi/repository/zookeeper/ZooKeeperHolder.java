package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ZooKeeperHolder {

    private static final int EXHIBITOR_RETRY_TIME = 1000;
    private static final int EXHIBITOR_RETRY_MAX = 3;
    private static final int EXHIBITOR_POLLING_MS = 300000;
    private static final String TYPE_EXHIBITOR = "exhibitor";
    private static final String TYPE_ZOOKEEPER = "zookeeper";

    private CuratorFramework curator;

    private ZooKeeperHolder(final CuratorFramework curator) {
        this.curator = curator;
    }

    public CuratorFramework get() {
        return curator;
    }

    /**
     * Parses string in form: (exhibitor|zookeeper):(hostname)[(,(hostname))*]:(port)/(root_path)
     * to parse connection string. For example:
     * <p>
     * exhibitor://ex1.zalando.de,ex2.zalando.de:8181/nakadi
     * - specifies that list of servers should be taken by exhibitor protocol on port 8181, instances
     * 127.0.0.1, 127.0.0.2, and root path is /nakadi
     * <p>
     * zookeeper://ex1.zalando.de,ex2.zalando.de,ex3.zalando.de:2181/nakadi
     * - means that there are 3 zookeepers ex1, ex2, ex3 with client port 2181, root path for curator library is
     * /nakadi
     *
     * @param zooConnectionString Connection string to parse
     * @return Zookeeper holder to use
     */
    public static ZooKeeperHolder build(final String zooConnectionString) {
        final String[] protocolAndProtocolData = zooConnectionString.split("://");
        if (protocolAndProtocolData.length != 2) {
            throw new RuntimeException("Connection string is wrong for " + zooConnectionString);
        }
        final int pathLocation = protocolAndProtocolData[1].indexOf('/');
        final String path;
        final String serversAndPort;
        if (pathLocation < 0) {
            path = "/";
            serversAndPort = protocolAndProtocolData[1];
        } else {
            path = protocolAndProtocolData[1].substring(pathLocation);
            serversAndPort = protocolAndProtocolData[1].substring(0, pathLocation);
        }

        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX);

        final CuratorFramework framework = CuratorFrameworkFactory.builder()
                .ensembleProvider(createEnsembleProvider(protocolAndProtocolData[0], serversAndPort, path))
                .retryPolicy(retryPolicy)
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .build();

        framework.start();
        return new ZooKeeperHolder(framework);
    }

    private static EnsembleProvider createEnsembleProvider(
            final String type, final String serversAndPort, final String path) {
        final EnsembleProvider result;
        if (TYPE_EXHIBITOR.equalsIgnoreCase(type)) {
            final String[] hostsPort = serversAndPort.split(":");
            if (hostsPort.length != 2) {
                throw new RuntimeException("Failed to parse zookeeper connection configuration, port not found in " +
                        serversAndPort);
            }
            final Exhibitors exhibitors = new Exhibitors(
                    Arrays.asList(hostsPort[0].split(",")),
                    Integer.parseInt(hostsPort[1]),
                    () -> {
                        throw new RuntimeException("No backup list exists");
                    });

            final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
            result = new ExhibitorEnsembleProvider(
                    exhibitors, exhibitorRestClient,
                    "/exhibitor/v1/cluster/list",
                    EXHIBITOR_POLLING_MS,
                    new ExponentialBackoffRetry(EXHIBITOR_RETRY_TIME, EXHIBITOR_RETRY_MAX),
                    path);
        } else if (TYPE_ZOOKEEPER.equalsIgnoreCase(type)) {
            final List<String> hosts = Arrays.asList(serversAndPort.split(","));
            Collections.shuffle(hosts);
            result = new FixedEnsembleProvider(String.join(",", hosts) + path);
        } else {
            throw new RuntimeException("Connection type for zookeeper ensemble provider is not supported: " + type);
        }
        return result;
    }

    private static class ExhibitorEnsembleProvider
            extends org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider {

        private final String zookeeperKafkaNamespace;

        ExhibitorEnsembleProvider(final Exhibitors exhibitors,
                                  final ExhibitorRestClient restClient,
                                  final String restUriPath,
                                  final int pollingMs,
                                  final RetryPolicy retryPolicy,
                                  final String zookeeperKafkaNamespace) {
            super(exhibitors, restClient, restUriPath, pollingMs, retryPolicy);
            this.zookeeperKafkaNamespace = zookeeperKafkaNamespace;
        }

        @Override
        public String getConnectionString() {
            return super.getConnectionString() + zookeeperKafkaNamespace;
        }
    }
}
