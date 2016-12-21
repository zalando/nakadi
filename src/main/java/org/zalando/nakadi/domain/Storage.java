package org.zalando.nakadi.domain;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    public abstract List<VersionedCursor> restorePosition(Integer timelineId, String data) throws IOException;

    public abstract String storePosition(List<VersionedCursor> cursors);

    public abstract Timeline.EventTypeConfiguration restoreEventTypeConfiguration(
            ObjectMapper objectMapper, String data) throws IOException;

    public abstract Map<String, Object> createSpecificConfiguration();

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Storage storage = (Storage) o;
        return Objects.equals(id, storage.id) && type == storage.type;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    public static class KafkaStorage extends Storage {
        private String zkAddress;
        private String zkPath;

        public KafkaStorage() {
            setType(Type.KAFKA);
        }

        public KafkaStorage(final String zkAddress, final String zkPath) {
            setType(Type.KAFKA);
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
        public List<VersionedCursor> restorePosition(
                final Integer timelineId, final String data) throws IOException {
            if (null == data) {
                return null;
            }
            final String[] items = data.split(",");
            return IntStream.range(0, items.length).mapToObj(
                    idx -> new VersionedCursor.VersionedCursorV1(
                            String.valueOf(idx),
                            timelineId,
                            items[idx]
                    )).collect(Collectors.toList());
        }

        @Override
        public String storePosition(final List<VersionedCursor> cursors) {
            if (null == cursors) {
                return null;
            }
            return cursors.stream().map(c -> {
                if (c instanceof VersionedCursor.VersionedCursorV0) {
                    return ((VersionedCursor.VersionedCursorV0) c).getOffset();
                } else if (c instanceof VersionedCursor.VersionedCursorV1) {
                    return ((VersionedCursor.VersionedCursorV1) c).getOffset();
                } else {
                    throw new IllegalArgumentException("Cursor version is not supported");
                }
            }).collect(Collectors.joining(","));
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

        @Override
        public boolean equals(final Object o) {
            if (!super.equals(o)) return false;

            final KafkaStorage that = (KafkaStorage) o;

            return Objects.equals(zkAddress, that.zkAddress) && Objects.equals(zkPath, that.zkPath);
        }
    }

}
