package org.zalando.nakadi.view;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CursorDistance {
    @Valid
    @NotNull
    private Cursor initialCursor;
    @Valid
    @NotNull
    private Cursor finalCursor;

    @Nullable
    private Long distance;

    public Cursor getInitialCursor() {
        return initialCursor;
    }

    public void setInitialCursor(final Cursor initialCursor) {
        this.initialCursor = initialCursor;
    }

    public Cursor getFinalCursor() {
        return finalCursor;
    }

    public void setFinalCursor(final Cursor finalCursor) {
        this.finalCursor = finalCursor;
    }

    public Long getDistance() {
        return distance;
    }

    public void setDistance(final Long distance) {
        this.distance = distance;
    }
}
