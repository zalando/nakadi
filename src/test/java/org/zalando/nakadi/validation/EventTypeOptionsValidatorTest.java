package org.zalando.nakadi.validation;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;

public class EventTypeOptionsValidatorTest {

    private static final Long TOPIC_RETENTION_MIN = 1L;
    private static final Long TOPIC_RETENTION_MAX = 3L;

    private final EventTypeOptionsValidator validator = new EventTypeOptionsValidator(
            TOPIC_RETENTION_MIN,
            TOPIC_RETENTION_MAX
    );

    @Test
    public void testValidationMin() {
        try {
            validator.checkRetentionTime(createEventTypeOptions(0L));
            Assert.fail("EventTypeOptionsValidationException is expected here");
        } catch (EventTypeOptionsValidationException e) {
            Assert.assertEquals(
                    "Field \"options.retention_time\" can not be less than " + TOPIC_RETENTION_MIN, e.getMessage());
        }
    }

    @Test
    public void testValidationMax() {
        try {
            validator.checkRetentionTime(createEventTypeOptions(4L));
            Assert.fail("EventTypeOptionsValidationException is expected here");
        } catch (EventTypeOptionsValidationException e) {
            Assert.assertEquals(
                    "Field \"options.retention_time\" can not be more than " + TOPIC_RETENTION_MAX, e.getMessage());
        }
    }

    private EventTypeOptions createEventTypeOptions(final long retentionTime) {
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(retentionTime);
        return eventTypeOptions;
    }

}
