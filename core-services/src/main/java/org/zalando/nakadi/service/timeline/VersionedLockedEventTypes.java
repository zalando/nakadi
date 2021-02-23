package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Data node for storing in zookeeper.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VersionedLockedEventTypes {
    private final Long version;
    private final Set<String> lockedEts;

    public static final VersionedLockedEventTypes EMPTY = new VersionedLockedEventTypes(0L, Collections.emptySet());

    @JsonCreator
    public VersionedLockedEventTypes(
            @JsonProperty("version") final Long version,
            @JsonProperty("locked_ets") final Set<String> lockedEts) {
        this.version = version;
        this.lockedEts = lockedEts;
    }

    @JsonProperty("version")
    public Long getVersion() {
        return version;
    }

    @JsonProperty("locked_ets")
    public Set<String> getLockedEts() {
        return lockedEts;
    }

    public byte[] serialize(final ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static VersionedLockedEventTypes deserialize(final ObjectMapper objectMapper, final byte[] data) {
        try {
            return objectMapper.readValue(data, VersionedLockedEventTypes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "VersionedLockedEventTypes{" +
                "version=" + version +
                ", lockedEts=" + lockedEts +
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
        VersionedLockedEventTypes that = (VersionedLockedEventTypes) o;
        return Objects.equals(version, that.version) && Objects.equals(lockedEts, that.lockedEts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, lockedEts);
    }
}
