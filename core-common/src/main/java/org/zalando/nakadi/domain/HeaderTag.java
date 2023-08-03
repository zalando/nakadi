package org.zalando.nakadi.domain;


import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum HeaderTag {
    SUBSCRIPTION_ID;

    private static final Map<String, HeaderTag> STRING_TO_ENUM = HeaderTag.
            stream().
            collect(Collectors.toMap(HeaderTag::name, Function.identity()));

    public static Optional<HeaderTag> fromString(final String consumerTag){
        return Optional.ofNullable(STRING_TO_ENUM.get(consumerTag));
    }

    private static Stream<HeaderTag> stream() {
        return Arrays.stream(values());
    }
}
