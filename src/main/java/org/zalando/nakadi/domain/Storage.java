package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;

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
        @JsonProperty("zk_address")
        private String zkAddress;
        @JsonProperty("zk_path")
        private String zkPath;

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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KafkaConfiguration)) {
                return false;
            }

            final KafkaConfiguration that = (KafkaConfiguration) o;
            return Objects.equals(zkAddress, that.zkAddress) && Objects.equals(zkPath, that.zkPath);
        }

        @Override
        public int hashCode() {
            int result = zkAddress != null ? zkAddress.hashCode() : 0;
            result = 31 * result + (zkPath != null ? zkPath.hashCode() : 0);
            return result;
        }
    }

    private String id;
    private Type type;
    private Object configuration;

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
        if (configuration == null) {
            return null;
        }
        if (!clazz.isAssignableFrom(configuration.getClass())) {
            throw new IllegalStateException("Can not cast configuration " + configuration + " to class " + clazz);
        }
        return (T) configuration;
    }

    public <T> void setConfiguration(final T configuration) {
        if (null != configuration && getType().configClass != configuration.getClass()) {
            throw new IllegalStateException("Only configuration of type " + getType().configClass + " accepted");
        }
        this.configuration = configuration;
    }

    public void parseConfiguration(final ObjectMapper mapper, final String data) throws IOException {
        this.configuration = null == data ? null : mapper.readValue(data, getType().configClass);
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
}
