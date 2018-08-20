package org.zalando.nakadi.service.converter;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;

import static org.mockito.Mockito.mock;

public class VersionZeroConverterTest {

    private TimelineService timelineService;
    private VersionedConverter converter;

    @Before
    public void setupMocks() {
        timelineService = mock(TimelineService.class);
        converter = new VersionZeroConverter(timelineService);
    }

    @Test(expected = InvalidCursorException.class)
    public void testInvalidChars() throws InternalNakadiException, InvalidCursorException, NoSuchEventTypeException,
            ServiceTemporarilyUnavailableException {
        final Cursor cursor = new Cursor("0", "-1-");
        converter.convert("event-type", cursor);
    }
}
