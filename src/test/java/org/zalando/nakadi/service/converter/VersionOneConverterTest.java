package org.zalando.nakadi.service.converter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.view.Cursor;

import java.util.Collections;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionOneConverterTest {
    private EventTypeCache eventTypeCache;
    private VersionedConverter converter;

    @Before
    public void setupMocks() {
        eventTypeCache = mock(EventTypeCache.class);
        converter = new VersionOneConverter(eventTypeCache);
    }

    @Test
    public void testInvalidCursorExceptionOnNotExistentTimeline() throws Exception {
        final Cursor cursor = new Cursor("1", "001-0002-012345");
        final String eventTypeName = "my_et";
        final Timeline firstTimeline = mock(Timeline.class);
        when(firstTimeline.getOrder()).thenReturn(1);
        final EventType eventType = mock(EventType.class);
        when(eventTypeCache.getTimelinesOrdered(eq(eventTypeName)))
                .thenReturn(Collections.singletonList(firstTimeline));

        try {
            converter.convert(eventTypeName, cursor);
            Assert.fail("Convert should throw exception on invalid cursor");
        } catch (final InvalidCursorException ex) {
            Assert.assertEquals(CursorError.UNAVAILABLE, ex.getError());
        }
    }

    @Test
    public void testCorrectParse() throws Exception {
        final Cursor cursor = new Cursor("1", "001-0010-012345");
        final String eventTypeName = "my_et";
        final Timeline firstTimeline = mock(Timeline.class);
        when(firstTimeline.getStorage()).thenReturn(new Storage("default", Storage.Type.KAFKA));
        when(firstTimeline.getOrder()).thenReturn(16);
        when(eventTypeCache.getTimelinesOrdered(eq(eventTypeName)))
                .thenReturn(Collections.singletonList(firstTimeline));
        final NakadiCursor nakadiCursor = converter.convert(eventTypeName, cursor);
        Assert.assertEquals(firstTimeline, nakadiCursor.getTimeline());
        Assert.assertEquals("1", nakadiCursor.getPartition());
        Assert.assertEquals("012345", nakadiCursor.getOffset());
    }

    @Test(expected = InvalidCursorException.class)
    public void testIncorrectValue1() throws Exception {
        converter.convert("my_et", new Cursor("1", "001-043"));
    }

    @Test(expected = InvalidCursorException.class)
    public void testIncorrectValue2() throws Exception {
        converter.convert("my_et", new Cursor("1", "001-fjur-48rre64545"));
    }

    @Test(expected = InvalidCursorException.class)
    public void testIncorrectOffset() throws Exception {
        converter.convert("my_et", new Cursor("1", "001-0010-xyz"));
    }

    @Test
    public void testFormatOffset() {
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getOrder()).thenReturn(15);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        final NakadiCursor cursor = NakadiCursor.of(timeline, "x", "012345");

        Assert.assertEquals(
                "001-000f-012345", new VersionOneConverter(null).formatOffset(cursor));
    }

}
