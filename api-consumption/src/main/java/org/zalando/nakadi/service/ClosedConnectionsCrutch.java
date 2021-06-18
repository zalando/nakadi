package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Feature;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

@Component
public class ClosedConnectionsCrutch {
    /**
     * List of allowed connection states
     * http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/include/net/tcp_states.h
     */
    enum ConnectionState {
        TCP_ESTABLISHED(1),
        TCP_SYN_SENT(2),
        TCP_SYN_RECV(3),
        TCP_FIN_WAIT1(4),
        TCP_FIN_WAIT2(5),
        TCP_TIME_WAIT(6),
        TCP_CLOSE(7),
        TCP_CLOSE_WAIT(8),
        TCP_LAST_ACK(9),
        TCP_LISTEN(10),
        TCP_CLOSING(11),    /* Now a valid state */
        TCP_NEW_SYN_RECV(12),
        ;
        private final int stateCode;

        ConnectionState(final int stateCode) {
            this.stateCode = stateCode;
        }

        public static ConnectionState fromCode(final String value) {
            final int intCode = Integer.parseInt(value, 16);
            for (final ConnectionState val : ConnectionState.values()) {
                if (val.stateCode == intCode) {
                    return val;
                }
            }
            throw new IllegalArgumentException("Failed to find connection state from " + value);
        }
    }

    static class ConnectionInfo {
        private final InetAddress address;
        private final int port;

        ConnectionInfo(final InetAddress address, final int port) {
            this.address = address;
            this.port = port;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ConnectionInfo that = (ConnectionInfo) o;
            return port == that.port && address.equals(that.address);
        }

        @Override
        public int hashCode() {
            int result = address.hashCode();
            result = 31 * result + port;
            return result;
        }

        @Override
        public String toString() {
            return "ConnectionInfo{" +
                    "address=" + address +
                    ", port=" + port +
                    '}';
        }
    }

    private final int port;
    private final Map<ConnectionInfo, List<BooleanSupplier>> listeners = new HashMap<>();
    private final Map<ConnectionInfo, List<BooleanSupplier>> toAdd = new HashMap<>();
    private final Meter meterClosed;
    private final FeatureToggleService featureToggleService;
    private static final Logger LOG = LoggerFactory.getLogger(ClosedConnectionsCrutch.class);

    @Autowired
    public ClosedConnectionsCrutch(
            @Value("${server.port}") final int port,
            final MetricRegistry metricRegistry,
            final FeatureToggleService featureToggleService) {
        this.port = port;
        this.meterClosed = metricRegistry.meter("nakadi.close_crutch.closed");
        this.featureToggleService = featureToggleService;
    }

    public AtomicBoolean listenForConnectionClose(final HttpServletRequest request)
            throws UnknownHostException {

        final AtomicBoolean connectionReady = new AtomicBoolean(true);
        listenForConnectionClose(
                InetAddress.getByName(request.getRemoteAddr()),
                request.getRemotePort(),
                () -> connectionReady.compareAndSet(true, false));
        return connectionReady;
    }

    public void listenForConnectionClose(
            final InetAddress address,
            final int port,
            final BooleanSupplier onCloseListener) {
        if (!featureToggleService.isFeatureEnabled(Feature.CONNECTION_CLOSE_CRUTCH)) {
            return;
        }
        LOG.debug("Listening for connection to close using crutch ({}:{})", address, port);
        synchronized (toAdd) {
            toAdd.computeIfAbsent(new ConnectionInfo(address, port), tmp -> new ArrayList<>()).add(onCloseListener);
        }
    }

    // Disabling as to validate that ClosedConnectionsCrutch is not needed in Springboot 2 version.
    //@Scheduled(fixedDelay = 1000)
    public void refresh() throws IOException {
        if (!featureToggleService.isFeatureEnabled(Feature.CONNECTION_CLOSE_CRUTCH)) {
            return;
        }
        synchronized (toAdd) {
            toAdd.forEach((conn, toAddListeners) -> {
                listeners.computeIfAbsent(conn, c -> new ArrayList<>()).addAll(toAddListeners);
            });
            toAdd.clear();
        }
        final Map<ConnectionInfo, ConnectionState> currentConnections = readAllConnectionStates();
        final long closedCount =
                new HashSet<>(listeners.keySet()).stream()
                        .filter(key -> CLOSED_STATES.contains(currentConnections.getOrDefault(key,
                                ConnectionState.TCP_CLOSE)))
                        .mapToLong(key -> {
                            LOG.debug("Notifying about connection close via crutch: {}", key);
                            return listeners.remove(key).stream().filter(BooleanSupplier::getAsBoolean).count();
                        }).sum();
        if (closedCount > 0) {
            meterClosed.mark(closedCount);
        }
    }

    private Map<ConnectionInfo, ConnectionState> readAllConnectionStates() throws IOException {
        final Map<ConnectionInfo, ConnectionState> result = new HashMap<>();
        for (final File f : new File[]{new File("/proc/net/tcp"), new File("/proc/net/tcp6")}) {
            try (FileInputStream in = new FileInputStream(f)) {
                result.putAll(getCurrentConnections(in));
            } catch (final FileNotFoundException e) {
                LOG.warn("Failed to find file " + f.getName() + ", skipping");
            }
        }
        return result;
    }

    @VisibleForTesting
    Map<ConnectionInfo, ConnectionState> getCurrentConnections(final InputStream in) throws IOException {
        final Map<ConnectionInfo, ConnectionState> connectionToState = new HashMap<>();
        // Linux proc filesystem files aren't ordinary files and have problems with readLine method
        // workaround problem by reading the contents to memory before processing
        final ByteArrayInputStream fullyReadInput = new ByteArrayInputStream(IOUtils.toByteArray(in));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fullyReadInput))) {
            // /proc/net/tcp[6]
            // idx, local_address, remote_address, status, ...

            reader.readLine(); // Skip heading

            String line;
            while (null != (line = reader.readLine())) {
                final StringTokenizer tokenizer = new StringTokenizer(line);
                // skip id
                tokenizer.nextToken();
                try {
                    // read local address
                    final String[] localAddress = tokenizer.nextToken().split(":");
                    final int localPort = Integer.parseInt(localAddress[1], 16);
                    if (localPort != port) {
                        continue;
                    }
                    // read remote address
                    final String[] remoteAddressPair = tokenizer.nextToken().split(":");
                    final InetAddress[] remoteAddresses = restoreAddresses(remoteAddressPair[0]);
                    final int remotePort = Integer.parseInt(remoteAddressPair[1], 16);

                    // read connection state
                    final ConnectionState connectionState = ConnectionState.fromCode(tokenizer.nextToken());
                    Stream.of(remoteAddresses)
                            .map(address -> new ConnectionInfo(address, remotePort))
                            .forEach(info -> connectionToState.put(info, connectionState));
                } catch (final DecoderException | RuntimeException ex) {
                    LOG.error("Failed to parse line, skipping: " + line, ex);
                }
            }
        }
        return connectionToState;
    }

    private static InetAddress[] restoreAddresses(final String address) throws DecoderException, UnknownHostException {
        final byte[] data = Hex.decodeHex(address.toCharArray());
        if (data.length == 16) {
            // /proc/net/tcp6 contains addresses in strange manner - each 4 bytes are rotated,
            // so we need to rotate it back
            for (int i = 0; i < 4; ++i) {
                ArrayUtils.reverse(data, i * 4, i * 4 + 4);
            }
            final InetAddress result = InetAddress.getByAddress(data);
            if (result instanceof Inet6Address) {
                if (InetAddresses.hasEmbeddedIPv4ClientAddress((Inet6Address) result)) {
                    return new InetAddress[]{result, InetAddresses.getEmbeddedIPv4ClientAddress((Inet6Address) result)};
                }
            }
            return new InetAddress[]{result};
        } else {
            ArrayUtils.reverse(data);
            return new InetAddress[]{InetAddress.getByAddress(data)};
        }
    }

    private static final Set<ConnectionState> CLOSED_STATES = ImmutableSet.of(
            ConnectionState.TCP_TIME_WAIT,
            ConnectionState.TCP_CLOSE_WAIT,
            ConnectionState.TCP_LAST_ACK,
            ConnectionState.TCP_CLOSING,
            ConnectionState.TCP_CLOSE);
}
