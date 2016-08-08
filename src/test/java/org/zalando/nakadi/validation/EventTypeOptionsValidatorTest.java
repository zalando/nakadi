package org.zalando.nakadi.validation;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.validation.Errors;
import org.zalando.nakadi.domain.EventTypeOptions;

public class EventTypeOptionsValidatorTest {

    private static final Long TOPIC_RETENTION_MIN = 1L;
    private static final Long TOPIC_RETENTION_MAX = 3L;

    private final Errors mockedErrors = Mockito.mock(Errors.class);
    private final EventTypeOptionsValidator validator = new EventTypeOptionsValidator(
            TOPIC_RETENTION_MIN,
            TOPIC_RETENTION_MAX
    );

    @Before
    public void setUp() throws Exception {
        Mockito.reset(mockedErrors);
    }

    @Test
    public void testValidation() {
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        validator.validate(eventTypeOptions, mockedErrors);

        Mockito.verify(mockedErrors, Mockito.times(0)).rejectValue(Matchers.any(), Matchers.any(),
                Matchers.any());
    }

    @Test
    public void testValidationMin() {
        validator.validate(createEventTypeOptions(0L), mockedErrors);

        Mockito.verify(mockedErrors, Mockito.times(1))
                .rejectValue("options.retentionTime", null, "can not be less than " + TOPIC_RETENTION_MIN);
    }

    @Test
    public void testValidationMax() {
        validator.validate(createEventTypeOptions(4L), mockedErrors);

        Mockito.verify(mockedErrors, Mockito.times(1))
                .rejectValue("options.retentionTime", null, "can not be more than " + TOPIC_RETENTION_MAX);
    }

    private EventTypeOptions createEventTypeOptions(final long retentionTime) {
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(retentionTime);
        return eventTypeOptions;
    }

}