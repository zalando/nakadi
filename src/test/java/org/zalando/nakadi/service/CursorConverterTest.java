package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.view.Cursor;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CursorConverterTest {
    @Test
    public void cursorShouldBePaddedWithZeroesWhenFeatureEnabled() {
        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(eq(FeatureToggleService.Feature.ZERO_PADDED_OFFSETS)))
                .thenReturn(true);

        final CursorConverter converter = new CursorConverter(featureToggleService);

        final NakadiCursor nakadiCursor = new NakadiCursor("test", "2", "1");
        final Cursor expectedCursor = new Cursor("2","000000000000000001");
        Assert.assertEquals(18, expectedCursor.getOffset().length());
        Assert.assertEquals(expectedCursor, converter.convert(nakadiCursor));
    }

    @Test
    public void cursorShouldNotBePaddedWithZeroesWhenFeatureDisabled() {
        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(eq(FeatureToggleService.Feature.ZERO_PADDED_OFFSETS)))
                .thenReturn(false);

        final CursorConverter converter = new CursorConverter(featureToggleService);

        final NakadiCursor nakadiCursor = new NakadiCursor("test", "0", "1");
        final Cursor expectedCursor = new Cursor("0","1");
        Assert.assertEquals(expectedCursor, converter.convert(nakadiCursor));
    }

}