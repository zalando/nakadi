package org.zalando.nakadi.util;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.regex.Pattern;

@Component
public class UUIDGenerator {

    public static final Pattern UUID_PATTERN = Pattern.compile
            ("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    public UUID randomUUID() {
        return UUID.randomUUID();
    }

    public UUID fromString(final String uuid) {
        return UUID.fromString(uuid);
    }

    public boolean isUUID(final String maybeUUID) {
        return UUID_PATTERN.matcher(maybeUUID).matches();
    }
}
