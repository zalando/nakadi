package org.zalando.nakadi.view;

import javax.annotation.concurrent.Immutable;

@Immutable
public class CursorCommitResult {

    public static final String COMMITTED = "committed";
    public static final String OUTDATED = "outdated";

    private final SubscriptionCursor cursor;
    private final String result;

    public CursorCommitResult(final SubscriptionCursor cursor, final boolean result) {
        this.cursor = cursor;
        this.result = result ? COMMITTED : OUTDATED;
    }

    public SubscriptionCursor getCursor() {
        return cursor;
    }

    public String getResult() {
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CursorCommitResult that = (CursorCommitResult) o;
        return cursor.equals(that.cursor) && result.equals(that.result);
    }

    @Override
    public int hashCode() {
        int hashCode = cursor.hashCode();
        hashCode = 31 * hashCode + result.hashCode();
        return hashCode;
    }
}
