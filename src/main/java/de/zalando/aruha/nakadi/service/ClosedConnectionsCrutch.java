package de.zalando.aruha.nakadi.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.InetAddresses;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import java.io.BufferedReader;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
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
        TCP_CLOSING(11),	/* Now a valid state */
        TCP_NEW_SYN_RECV(12),;
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

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

    static class ListenerInfo implements Comparable<ListenerInfo> {
        private final Date addedAt;
        private final ConnectionInfo connectionInfo;
        private final BooleanSupplier callback;

        public ListenerInfo(final Date addedAt, final ConnectionInfo connectionInfo, final BooleanSupplier callback) {
            this.addedAt = addedAt;
            this.connectionInfo = connectionInfo;
            this.callback = callback;
        }

        @Override
        public int compareTo(final ListenerInfo o) {
            return addedAt.compareTo(o.addedAt);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final ListenerInfo that = (ListenerInfo) o;

            if (addedAt != that.addedAt) return false;
            if (!connectionInfo.equals(that.connectionInfo)) return false;
            return callback.equals(that.callback);

        }

        @Override
        public int hashCode() {
            final long value = addedAt.getTime();
            return (int) (value ^ (value >>> 32));
        }
    }

    private final int port;
    private final Map<ConnectionInfo, List<BooleanSupplier>> listeners = new HashMap<>();
    private final SortedSet<ListenerInfo> toAdd = new TreeSet<>();
    private final Meter meterClosed;
    private final FeatureToggleService featureToggleService;
    private static final Logger LOG = LoggerFactory.getLogger(ClosedConnectionsCrutch.class);
    private static final long DELAY_MILLIS = 100;

    private static final String FEATURE_NAME = "close_crutch";
    @Autowired
    public ClosedConnectionsCrutch(
            @Value("${server.port}") final int port,
            final MetricRegistry metricRegistry,
            final FeatureToggleService featureToggleService) {
        this.port = port;
        this.meterClosed = metricRegistry.meter("nakadi.close_crutch.closed");
        this.featureToggleService = featureToggleService;
    }

    public void listenForConnectionClose(
            final InetAddress address,
            final int port,
            final BooleanSupplier onCloseListener) {
        if (!featureToggleService.isFeatureEnabled(FEATURE_NAME)) {
            return;
        }
        LOG.debug("Listening for connection to close using crutch (" + address + ":" + port + ")");
        final Date now = new Date();
        synchronized (this) {
            toAdd.add(new ListenerInfo(now, new ConnectionInfo(address, port), onCloseListener));
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void refresh() throws IOException {
        if (!featureToggleService.isFeatureEnabled(FEATURE_NAME)) {
            return;
        }
        final long selector = System.currentTimeMillis() - DELAY_MILLIS;
        synchronized (this) {
            final Iterator<ListenerInfo> it = toAdd.iterator();
            while (it.hasNext()) {
                final ListenerInfo first = it.next();
                if (first.addedAt.getTime() >= selector) {
                    break;
                }
                listeners.computeIfAbsent(first.connectionInfo, info -> new ArrayList<>()).add(first.callback);
                it.remove();
            }
        }
        final Map<ConnectionInfo, ConnectionState> currentConnections = readAllConnectionStates();
        final Set<ConnectionInfo> keys = new HashSet<>(listeners.keySet());
        final AtomicInteger closedCount = new AtomicInteger();
        for (final ConnectionInfo key : keys) {
            if (CLOSED_STATES.contains(currentConnections.getOrDefault(key, ConnectionState.TCP_CLOSE))) {
                LOG.info("Notifying about connection close via crutch: " + key);
                listeners.remove(key).forEach(callable -> {
                    if (callable.getAsBoolean()) {
                        closedCount.incrementAndGet();
                    }
                });
            }
        }
        if (closedCount.get() > 0) {
            meterClosed.mark(closedCount.get());
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

    Map<ConnectionInfo, ConnectionState> getCurrentConnections(final InputStream in) throws IOException {
        final Map<ConnectionInfo, ConnectionState> connectionToState = new HashMap<>();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
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
                    for (final InetAddress tmp : remoteAddresses) {
                        connectionToState.put(new ConnectionInfo(tmp, remotePort), connectionState);
                    }
                } catch (DecoderException | RuntimeException ex) {
                    LOG.error("Failed to parse line, skipping: " + line, ex);
                }
            }
        }
        return connectionToState;
    }

    static InetAddress[] restoreAddresses(final String address) throws DecoderException, UnknownHostException {
        final byte[] data = Hex.decodeHex(address.toCharArray());
        if (data.length == 16) {
            // /proc/net/tcp6 contains addresses in strange manner - each 4 bytes are rotated, so we need to rotate it back
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
            return new InetAddress[]{InetAddress.getByAddress(data)};
        }
    }

    private static final Set<ConnectionState> CLOSED_STATES = Arrays.asList(
            ConnectionState.TCP_TIME_WAIT,
            ConnectionState.TCP_CLOSE_WAIT,
            ConnectionState.TCP_LAST_ACK,
            ConnectionState.TCP_CLOSING,
            ConnectionState.TCP_CLOSE)
            .stream().collect(Collectors.toSet());
}
