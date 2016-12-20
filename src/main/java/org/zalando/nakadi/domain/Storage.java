package org.zalando.nakadi.domain;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class Storage {
    public static enum Type {
        KAFKA
    }

    @NotNull
    private String id;
    @NotNull
    private Type type;

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

    public abstract Timeline.StoragePosition restorePosition(
            ObjectMapper objectMapper, String data) throws IOException;

    public abstract Timeline.EventTypeConfiguration restoreEventTypeConfiguration(
            ObjectMapper objectMapper, String data) throws IOException;

    public abstract Map<String, Object> createSpecificConfiguration();

    public static class KafkaStorage extends Storage {
        private String zkAddress;
        private String zkPath;

        public KafkaStorage() {
            setType(Type.KAFKA);
        }

        public KafkaStorage(final String zkAddress, final String zkPath) {
            this.zkAddress = zkAddress;
            this.zkPath = zkPath;
        }

        public String getZkAddress() {
            return zkAddress;
        }

        public String getZkPath() {
            return zkPath;
        }

        @Override
        public Timeline.KafkaStoragePosition restorePosition(
                final ObjectMapper objectMapper, final String data) throws IOException {
            return null == data ? null : objectMapper.readValue(data, Timeline.KafkaStoragePosition.class);
        }

        @Override
        public Timeline.KafkaEventTypeConfiguration restoreEventTypeConfiguration(
                final ObjectMapper objectMapper, final String data) throws IOException {
            return null == data ? null : objectMapper.readValue(data, Timeline.KafkaEventTypeConfiguration.class);
        }

        @Override
        public Map<String, Object> createSpecificConfiguration() {
            final Map<String, Object> result = new HashMap<>();
            result.put("zk_address", zkAddress);
            result.put("zk_path", zkPath);
            return result;
        }
    }

}
