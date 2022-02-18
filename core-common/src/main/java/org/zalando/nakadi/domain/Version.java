package org.zalando.nakadi.domain;

public interface Version {
    enum Level { MAJOR, MINOR, PATCH, NO_CHANGES }

    Version bump(final Version.Level level);

}
