package org.zalando.nakadi.domain.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KafkaConfiguration {
    private final ZookeeperConnection zookeeperConnection;

    public KafkaConfiguration(
            @JsonProperty(value = "zookeeper_connection") final ZookeeperConnection zookeeperConnection) {
        this.zookeeperConnection = zookeeperConnection;
    }

    public ZookeeperConnection getZookeeperConnection() {
        return zookeeperConnection;
    }

    @Override
    public String toString() {
        return "KafkaConfiguration{" + zookeeperConnection + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaConfiguration that = (KafkaConfiguration) o;
        return Objects.equals(zookeeperConnection, that.zookeeperConnection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zookeeperConnection);
    }
}
