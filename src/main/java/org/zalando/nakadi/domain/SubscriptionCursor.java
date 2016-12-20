package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Immutable
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class SubscriptionCursor extends Cursor {

    @NotNull
    private final String eventType;

    @NotNull
    private final String cursorToken;

    public SubscriptionCursor(@JsonProperty("partition") final String partition,
            @JsonProperty("offset") final String offset,
            @JsonProperty("event_type") final String eventType,
            @JsonProperty("cursor_token") final String cursorToken) {
        super(partition, offset);
        this.eventType = eventType;
        this.cursorToken = cursorToken;
    }
}
