package org.zalando.nakadi.util;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UUIDGenerator {

    public UUID randomUUID() {
        return UUID.randomUUID();
    }

    public UUID fromString(final String uuid) {
        return UUID.fromString(uuid);
    }

}
