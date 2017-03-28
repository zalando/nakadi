package org.zalando.nakadi.view;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CursorDistanceQuery {
    @Valid
    @NotNull
    private Cursor initialCursor;
    @Valid
    @NotNull
    private Cursor finalCursor;

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
}
