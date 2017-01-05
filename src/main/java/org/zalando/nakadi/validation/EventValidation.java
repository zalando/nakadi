package org.zalando.nakadi.validation;

import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;

public class EventValidation {
    public static EventTypeValidator forType(final EventType eventType) {
        final EventTypeValidator etv = new EventTypeValidator(eventType);

        final ValidationStrategyConfiguration vsc = new ValidationStrategyConfiguration();
        vsc.setStrategyName(EventBodyMustRespectSchema.NAME);
        etv.withConfiguration(vsc);

        if (eventType.getCategory() == EventCategory.BUSINESS || eventType.getCategory() == EventCategory.DATA) {
            final ValidationStrategyConfiguration metadataConf = new ValidationStrategyConfiguration();
            metadataConf.setStrategyName(EventMetadataValidationStrategy.NAME);
            etv.withConfiguration(metadataConf);
        }

        return etv;
    }
}

