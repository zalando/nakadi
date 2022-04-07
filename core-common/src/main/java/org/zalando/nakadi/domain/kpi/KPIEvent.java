package org.zalando.nakadi.domain.kpi;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public abstract class KPIEvent {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface KPIField {
        String value();

        String getter() default "";
    }

    private final String eventTypeOfThisKPIEvent;

    protected KPIEvent(final String eventTypeOfThisKPIEvent) {
        this.eventTypeOfThisKPIEvent = eventTypeOfThisKPIEvent;
    }

    public final String eventTypeOfThisKPIEvent() {
        return this.eventTypeOfThisKPIEvent;
    }
}
