package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

public class AvroVersion implements Version {

    private final short version;

    @JsonCreator
    public AvroVersion(final String version){
        this.version = Short.parseShort(version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvroVersion that = (AvroVersion) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version);
    }

    @Override
    @JsonValue
    public String toString() {
        return Short.toString(version);
    }

    @Override
    public Version bump(Level level) { // ignores level
        return new AvroVersion(Short.toString((short) (this.version + 1)));
    }
}
