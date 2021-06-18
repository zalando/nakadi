package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class ShiftedCursor extends Cursor {
    @NotNull
    private long shift;

    public ShiftedCursor(@JsonProperty("partition") final String partition,
                         @JsonProperty("offset") final String offset) {
        super(partition, offset);
    }

    public long getShift() {
        return shift;
    }

    public void setShift(final long shift) {
        this.shift = shift;
    }
}
