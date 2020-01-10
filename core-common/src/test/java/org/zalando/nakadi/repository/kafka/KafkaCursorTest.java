package org.zalando.nakadi.repository.kafka;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.utils.TestUtils;

public class KafkaCursorTest {

    @Test
    public void testFromNakadiCursorCorrect() throws InvalidCursorException {
        Assert.assertEquals(new KafkaCursor("t1", 1, 123L),
                KafkaCursor.fromNakadiCursor(NakadiCursor.of(TestUtils.buildTimelineWithTopic("t1"), "1", "123")));
    }

    @Test
    public void testFromNakadiCursorBadPartition() {
        try {
            KafkaCursor.fromNakadiCursor(NakadiCursor.of(TestUtils.buildTimelineWithTopic("t1"), "x", "123"));
            Assert.fail("Cursor should not be parsed");
        } catch (final InvalidCursorException ex) {
            Assert.assertEquals(ex.getError(), CursorError.PARTITION_NOT_FOUND);
        }
    }

    @Test
    public void testFromNakadiCursorBadOffset() {
        try {
            KafkaCursor.fromNakadiCursor(NakadiCursor.of(TestUtils.buildTimelineWithTopic("t1"), "0", "x"));
            Assert.fail("Cursor should not be parsed");
        } catch (final InvalidCursorException ex) {
            Assert.assertEquals(ex.getError(), CursorError.INVALID_FORMAT);
        }
    }
}
