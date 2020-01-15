package org.zalando.nakadi.domain.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class AddressPort {
    private final String address;
    private final int port;

    public AddressPort(
            @JsonProperty("address") final String address,
            @JsonProperty("port") final int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String asAddressPort() {
        return address + ":" + port;
    }

    @Override
    public String toString() {
        return "AddressPort{" +
                "address='" + address + '\'' +
                ", port=" + port +
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
        final AddressPort that = (AddressPort) o;
        return port == that.port &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
