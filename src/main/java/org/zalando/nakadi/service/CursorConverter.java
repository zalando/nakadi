package org.zalando.nakadi.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;

@Service
public class CursorConverter {
    private final FeatureToggleService featureToggleService;

    public static final int CURSOR_OFFSET_LENGTH = 18;

    @Autowired
    public CursorConverter(final FeatureToggleService featureToggleService) {
        this.featureToggleService = featureToggleService;
    }

    public Cursor convert(final NakadiCursor nakadiCursor) {
        final boolean zeroPaddedOffsets =
                featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.ZERO_PADDED_OFFSETS);
        final String offset;
        if (nakadiCursor.getOffset().equals("-1")) {
            offset = Cursor.BEFORE_OLDEST_OFFSET;
        } else {
            if (zeroPaddedOffsets) {
                offset = StringUtils.leftPad(nakadiCursor.getOffset(), CURSOR_OFFSET_LENGTH, '0');
            } else {
                offset = nakadiCursor.getOffset();
            }
        }
        return new Cursor(
                nakadiCursor.getPartition(),
                offset);
    }

    public SubscriptionCursor convert(
            final NakadiCursor position, final String eventType, final String token) {
        final Cursor oldCursor = convert(position);
        return new SubscriptionCursor(oldCursor.getPartition(), oldCursor.getOffset(), eventType, token);
    }

}
