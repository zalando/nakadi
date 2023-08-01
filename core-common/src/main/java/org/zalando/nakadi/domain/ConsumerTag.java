package org.zalando.nakadi.domain;

import org.checkerframework.checker.units.qual.C;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public enum ConsumerTag {
    SUBSCRIPTION_ID;

    public static Optional<ConsumerTag> fromString(final String consumerTag){
        return Arrays.stream(values()).
                filter(ct -> consumerTag.equalsIgnoreCase(ct.name())).
                findFirst();
    }

    public static Stream<ConsumerTag> stream() {
        return Arrays.stream(values());
    }

}
