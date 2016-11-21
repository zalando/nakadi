package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class Version {
    private final int major;
    private final int minor;
    private final int patch;

    public Version(final int major, final int minor, final int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    @JsonCreator
    public Version(final String versionString) {
        final String[] versionStringParts = versionString.split("\\.");
        this.major = Integer.valueOf(versionStringParts[0]);
        this.minor = Integer.valueOf(versionStringParts[1]);
        this.patch = Integer.valueOf(versionStringParts[2]);
    }

    @Override
    @JsonValue
    public String toString() {
        return major + "." + minor + "." + patch;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Version version = (Version) o;

        if (major != version.major) return false;
        if (minor != version.minor) return false;
        return patch == version.patch;

    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + minor;
        result = 31 * result + patch;
        return result;
    }

    public Version bumpMinor() {
        return new Version(this.major, this.minor + 1, 0);
    }
}
