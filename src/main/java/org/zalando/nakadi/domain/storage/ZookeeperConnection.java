package org.zalando.nakadi.domain.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonIgnore;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ZookeeperConnection {

    private final ZookeeperConnectionType type;

    private final List<AddressPort> addresses;

    private final String path;

    public ZookeeperConnection(
            @JsonProperty("type") final ZookeeperConnectionType type,
            @JsonProperty("addresses") final List<AddressPort> addresses,
            @JsonProperty("path") final String path) {
        this.type = type;
        this.addresses = addresses;
        this.path = path;
    }

    public ZookeeperConnectionType getType() {
        return type;
    }

    public List<AddressPort> getAddresses() {
        return addresses;
    }

    @Nullable
    public String getPath() {
        return path;
    }

    @JsonIgnore
    public String getPathPrepared() {
        if (null == path) {
            return "";
        } else {
            return path.startsWith("/") ? path : ("/" + path);
        }
    }

    @Override
    public String toString() {
        return "ZookeeperConnection{" +
                "type=" + type +
                ", addresses=" + addresses +
                ", path='" + path + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ZookeeperConnection that = (ZookeeperConnection) o;
        return type == that.type &&
                Objects.equals(addresses, that.addresses) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, addresses, path);
    }

    private static final Pattern ZK_CONN_PATTERN =
            Pattern.compile("^(\\w+)://([\\w,:\\.-]*)(/[\\w\\-\\./]*)?$");

    public static ZookeeperConnection valueOf(final String connStr) {
        final Matcher m = ZK_CONN_PATTERN.matcher(connStr);
        if (!m.matches()) {
            throw new RuntimeException("Unsupported format of zk connection string " + connStr);
        }
        final String typeString = m.group(1);
        final ZookeeperConnectionType type = Arrays.stream(ZookeeperConnectionType.values())
                .filter(v -> v.name().equalsIgnoreCase(typeString))
                .findAny()
                .orElseThrow(() ->
                        new RuntimeException("Can not detect type of connection" + typeString + " in " + connStr));

        final String[] hostsPorts = m.group(2).split(",");
        final List<Integer> ports = Arrays.stream(hostsPorts)
                .filter(v -> v.contains(":"))
                .map(v -> v.substring(v.lastIndexOf(':') + 1))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        if (ports.isEmpty()) {
            throw new RuntimeException("Ports are not defined for hosts, no default port could be found in " + connStr);
        }
        final Integer defaultPort = ports.get(ports.size() - 1);
        final List<AddressPort> addresses = Arrays.stream(hostsPorts).map(hostPort -> {
            final int sepIndex = hostPort.lastIndexOf(':');
            if (sepIndex > 0) {
                return new AddressPort(
                        hostPort.substring(0, sepIndex), Integer.parseInt(hostPort.substring(sepIndex + 1)));
            } else {
                return new AddressPort(hostPort, defaultPort);
            }

        }).collect(Collectors.toList());
        final String path = m.groupCount() >= 3 ? m.group(3) : null;

        return new ZookeeperConnection(type, addresses, path);

    }
}
