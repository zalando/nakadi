package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.ensemble.EnsembleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Zookeeper 3.5 introduced configuration updates. Unfortunately, this configuration updates are cleaning chroot path
 * from zookeeper FixedEnsembleProvider. In order to avoid it - this ensemble provider was written. It tries to keep
 * chrooted path even when configuration was updated.
 */
public class ChrootedFixedEnsembleProvider implements EnsembleProvider {

    private final String chrootPath;
    private volatile String preparedConnection;
    private static final Logger LOG = LoggerFactory.getLogger(ChrootedFixedEnsembleProvider.class);

    public ChrootedFixedEnsembleProvider(final String addressesJoined, final String chrootPath) {
        this.chrootPath = chrootPath;
        this.preparedConnection = addressesJoined + chrootPath;
    }

    @Override
    public void start() throws Exception {
        // Do nothing
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    @Override
    public String getConnectionString() {
        return preparedConnection;
    }

    @Override
    public void setConnectionString(final String connectionString) {
        final String oldPreparedConnection = this.preparedConnection;
        if (connectionString.contains("/")) {
            this.preparedConnection = connectionString;
        } else {
            this.preparedConnection = connectionString + chrootPath;
        }
        LOG.info("Updated ensemble provider connection from {} to {}", oldPreparedConnection, this.preparedConnection);
    }

    @Override
    public boolean updateServerListEnabled() {
        return true;
    }
}
