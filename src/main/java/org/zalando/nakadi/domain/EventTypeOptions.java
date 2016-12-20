package org.zalando.nakadi.domain;

import javax.annotation.Nullable;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EventTypeOptions {

    @Nullable
    private Long retentionTime;
}
