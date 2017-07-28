package org.zalando.nakadi.service.converter;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;

import static org.mockito.Mockito.mock;

public class VersionZeroConverterTest {

    private TimelineService timelineService;
    private EventTypeCache eventTypeCache;
    private VersionedConverter converter;

    @Before
    public void setupMocks() {
        timelineService = mock(TimelineService.class);
        eventTypeCache = mock(EventTypeCache.class);
        converter = new VersionZeroConverter(eventTypeCache, timelineService);
    }

    @Test(expected = InvalidCursorException.class)
    public void testInvalidChars() throws InternalNakadiException, InvalidCursorException, NoSuchEventTypeException,
            ServiceUnavailableException {
        final Cursor cursor = new Cursor("0", "-1-");
        converter.convert("event-type", cursor);
    }
}