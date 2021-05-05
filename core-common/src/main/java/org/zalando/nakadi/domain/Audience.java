package org.zalando.nakadi.domain;

public enum Audience {
    COMPONENT_INTERNAL("component-internal"),
    BUSINESS_UNIT_INTERNAL("business-unit-internal"),
    COMPANY_INTERNAL("company-internal"),
    EXTERNAL_PARTNER("external-partner"),
    EXTERNAL_PUBLIC("external-public");

    private final String text;

    Audience(final String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static Audience fromString(final String text) {
        for (final Audience audience : Audience.values()) {
            if (audience.text.equals(text)) {
                return audience;
            }
        }
        throw new IllegalArgumentException();
    }
}
