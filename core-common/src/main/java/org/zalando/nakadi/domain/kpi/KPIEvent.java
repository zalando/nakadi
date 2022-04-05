package org.zalando.nakadi.domain.kpi;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public abstract class KPIEvent {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface KPIField {
        String value();

        String getter() default "";
    }

    /*
        Implementation of this method should return the event-type name of the specific KPI event.
        This method should not be confused with the getter "getEventType()" in some specific KPI events;
        which is why this method is named "eventTypeOfThisKPIEvent()
     */
    public abstract String eventTypeOfThisKPIEvent();
}
