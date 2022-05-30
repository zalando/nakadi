package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public abstract class KPIEvent {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface KPIField {
        String value();

        String getter() default "";
    }

    private final String name;

    protected KPIEvent(final String name) {
        this.name = name;
    }

    public final String getName() {
        return this.name;
    }

    public abstract Schema getSchema();
}
