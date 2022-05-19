package org.zalando.nakadi.domain.kpi;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public abstract class KPIEvent {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface KPIField {
        String value();

        String getter() default "";
    }

    private final String name;
    private final String version;

    protected KPIEvent(final String name,
                       final String version) {
        this.name = name;
        this.version = version;
    }

    public final String getName() {
        return this.name;
    }

    public String getVersion() {
        return version;
    }
}
