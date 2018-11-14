package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class Storage {
    public enum Type {
        KAFKA(KafkaConfiguration.class, Timeline.KafkaStoragePosition.class);
        private final Class configClass;
        private final Class<? extends Timeline.StoragePosition> positionClass;

        Type(final Class configClass, final Class<? extends Timeline.StoragePosition> positionClass) {
            this.configClass = configClass;
            this.positionClass = positionClass;
        }
    }

    public static class KafkaConfiguration {
        private String exhibitorAddress;
        private Integer exhibitorPort;
        private String zkAddress;
        private String zkPath;

        public KafkaConfiguration(
                @JsonProperty(value="exhibitor_address") final String exhibitorAddress,
                @JsonProperty(value="exhibitor_port", defaultValue = "8181") final Integer exhibitorPort,
                @JsonProperty(value="zk_address", defaultValue = "zookeeper:2181") final String zkAddress,
                @JsonProperty(value="zk_path", defaultValue = "") final String zkPath) {
            this.exhibitorAddress = exhibitorAddress;
            this.exhibitorPort = exhibitorPort;
            this.zkAddress = zkAddress;
            this.zkPath = zkPath;
        }

        public String getZkAddress() {
            return zkAddress;
        }

        public void setZkAddress(final String zkAddress) {
            this.zkAddress = zkAddress;
        }

        public String getZkPath() {
            return zkPath;
        }

        public void setZkPath(final String zkPath) {
            this.zkPath = zkPath;
        }

        public String getExhibitorAddress() {
            return exhibitorAddress;
        }

        public void setExhibitorAddress(final String exhibitorAddress) {
            this.exhibitorAddress = exhibitorAddress;
        }

        public Integer getExhibitorPort() {
            return exhibitorPort;
        }

        public void setExhibitorPort(final Integer exhibitorPort) {
            this.exhibitorPort = exhibitorPort;
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
            return Objects.equals(exhibitorAddress, that.exhibitorAddress) &&
                    Objects.equals(exhibitorPort, that.exhibitorPort) &&
                    Objects.equals(zkAddress, that.zkAddress) &&
                    Objects.equals(zkPath, that.zkPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exhibitorAddress, exhibitorPort, zkAddress, zkPath);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("KafkaConfiguration{");
            sb.append("exhibitorAddress='").append(exhibitorAddress).append('\'');
            sb.append(", exhibitorPort=").append(exhibitorPort);
            sb.append(", zkAddress='").append(zkAddress).append('\'');
            sb.append(", zkPath='").append(zkPath).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    private String id;
    @JsonProperty("storage_type")
    private Type type;
    private Object configuration;

    public Storage() {
    }

    public Storage(final String id, final Type type) {
        this.id = id;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public KafkaConfiguration getKafkaConfiguration() {
        return getConfiguration(KafkaConfiguration.class);
    }

    public <T> T getConfiguration(final Class<T> clazz) {
        if (!clazz.isAssignableFrom(configuration.getClass())) {
            throw new IllegalStateException("Can not cast configuration " + configuration + " to class " + clazz);
        }
        return (T) configuration;
    }

    public <T> void setConfiguration(final T configuration) {
        if (getType().configClass != configuration.getClass()) {
            throw new IllegalStateException("Only configuration of type " + getType().configClass + " accepted");
        }
        this.configuration = configuration;
    }

    public void parseConfiguration(final ObjectMapper mapper, final String data) throws IOException {
        this.configuration = mapper.readValue(data, getType().configClass);
    }

    @Nullable
    public Timeline.StoragePosition restorePosition(
            final ObjectMapper mapper, @Nullable final String data) throws IOException {
        return null == data ? null : mapper.readValue(data, getType().positionClass);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Storage)) {
            return false;
        }

        final Storage that = (Storage) o;

        return Objects.equals(id, that.id) &&
                Objects.equals(type, that.type) &&
                Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Storage{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", configuration=" + configuration +
                '}';
    }

}
