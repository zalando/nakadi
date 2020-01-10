package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class StreamMetadata {
    private final String debug;

    public StreamMetadata(@Nullable @JsonProperty("debug") final String debug) {
        this.debug = debug;
    }

    @Nullable
    public String getDebug() {
        return debug;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StreamMetadata that = (StreamMetadata) o;

        return debug != null ? debug.equals(that.debug) : that.debug == null;

    }

    @Override
    public int hashCode() {
        return debug != null ? debug.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "debug=" + debug;
    }
}
