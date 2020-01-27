package org.zalando.nakadi.service.subscription.autocommit;

import org.zalando.nakadi.domain.NakadiCursor;

class CursorInterval {
    private final NakadiCursor begin;
    private NakadiCursor end;

    CursorInterval(final NakadiCursor begin) {
        this.begin = begin;
        this.end = begin;
    }

    public NakadiCursor getBegin() {
        return begin;
    }

    public NakadiCursor getEnd() {
        return end;
    }

    public void setEnd(final NakadiCursor end) {
        this.end = end;
    }
}
