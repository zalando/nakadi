package org.zalando.nakadi.domain;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public abstract class Storage {
    public static enum Type {
        KAFKA
    }

    private String id;
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

    public static class KafkaStorage extends Storage {
        private String zkAddress;
        private String zkPath;

        public KafkaStorage() {
            setType(Type.KAFKA);
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
    }

}
