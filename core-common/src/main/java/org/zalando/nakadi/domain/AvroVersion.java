package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

public class AvroVersion implements Version {

    private final short version;

    public AvroVersion(){
        version = 1;
    }

    @JsonCreator
    public AvroVersion(final String version){
        this.version = Short.parseShort(version);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        final AvroVersion that = (AvroVersion) o;
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
    public Version bump(final Level level) {
        if(level != Level.NO_CHANGES){
            return new AvroVersion(Short.toString((short) (this.version + 1)));
        }

        return new AvroVersion(Short.toString((this.version)));
    }
}
