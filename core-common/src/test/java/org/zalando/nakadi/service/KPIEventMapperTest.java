package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;

import java.util.Set;
import java.util.UUID;

/*
    NOTE: Testing KPIEventMapper partially. Tests for the KPIEvent classes,
    like AccessLogEventTest, also test KPIEventMapper.
 */
public class KPIEventMapperTest {

    public static class ValidTestKPIEvent extends KPIEvent {
        @KPIField("simple_getter")
        private String simpleGetter;
        @KPIField(value = "explicit_getter", getter = "valueOfExplicitGetter")
        private String explicitGetter;
        @KPIField("boolean_getter")
        private Boolean booleanGetter;
        @KPIField("boolean_is_getter")
        private boolean booleanIsGetter;
        @KPIField("int_getter")
        private int intGetter;
        @KPIField("double_getter")
        private double doubleGetter;

        public ValidTestKPIEvent() {
            super("test-et");
        }

        public String getSimpleGetter() {
            return simpleGetter;
        }

        public ValidTestKPIEvent setSimpleGetter(final String simpleGetter) {
            this.simpleGetter = simpleGetter;
            return this;
        }

        public String valueOfExplicitGetter() {
            return explicitGetter;
        }

        public ValidTestKPIEvent setExplicitGetter(final String explicitGetter) {
            this.explicitGetter = explicitGetter;
            return this;
        }

        public Boolean getBooleanGetter() {
            return booleanGetter;
        }

        public ValidTestKPIEvent setBooleanGetter(final Boolean booleanGetter) {
            this.booleanGetter = booleanGetter;
            return this;
        }

        public boolean isBooleanIsGetter() {
            return booleanIsGetter;
        }

        public ValidTestKPIEvent setBooleanIsGetter(final boolean booleanIsGetter) {
            this.booleanIsGetter = booleanIsGetter;
            return this;
        }

        public int getIntGetter() {
            return intGetter;
        }

        public ValidTestKPIEvent setIntGetter(final int intGetter) {
            this.intGetter = intGetter;
            return this;
        }

        public double getDoubleGetter() {
            return doubleGetter;
        }

        public ValidTestKPIEvent setDoubleGetter(final double doubleGetter) {
            this.doubleGetter = doubleGetter;
            return this;
        }
    }

    public static class KPIEventWithoutGetter extends KPIEvent {
        @KPIField("test_field")
        private String testField;

        public KPIEventWithoutGetter setTestField(final String testField) {
            this.testField = testField;
            return this;
        }

        public KPIEventWithoutGetter() {
            super("test-et");
        }
    }

    public static class KPIEventWithPrivateGetter extends KPIEvent {
        @KPIField("test_field")
        private String testField;

        private String getTestField() {
            return testField;
        }

        public KPIEventWithPrivateGetter setTestField(final String testField) {
            this.testField = testField;
            return this;
        }

        public KPIEventWithPrivateGetter() {
            super("test-et");
        }
    }

    @Test
    public void testKPIEventMapperForValidKPIEvent() {
        final var validKPIEvent = new ValidTestKPIEvent()
                .setSimpleGetter(UUID.randomUUID().toString())
                .setExplicitGetter(UUID.randomUUID().toString())
                .setBooleanGetter(false)
                .setBooleanIsGetter(true)
                .setIntGetter(1357)
                .setDoubleGetter(24.68);

        final var eventMapper = new KPIEventMapper(Set.of(ValidTestKPIEvent.class));

        final var mappedEvent = eventMapper.mapToJsonObject(validKPIEvent);

        Assert.assertEquals(validKPIEvent.getSimpleGetter(), mappedEvent.get("simple_getter"));
        Assert.assertEquals(validKPIEvent.valueOfExplicitGetter(), mappedEvent.get("explicit_getter"));
        Assert.assertEquals(validKPIEvent.getBooleanGetter(), mappedEvent.get("boolean_getter"));
        Assert.assertEquals(validKPIEvent.isBooleanIsGetter(), mappedEvent.get("boolean_is_getter"));
        Assert.assertEquals(validKPIEvent.getIntGetter(), mappedEvent.get("int_getter"));
        Assert.assertEquals(validKPIEvent.getDoubleGetter(), mappedEvent.get("double_getter"));
    }

    @Test
    public void testKPIEventMapperThrowsWhenKPIEventNotInitialized() {
        final var validKPIEvent = new ValidTestKPIEvent();
        final var eventMapper = new KPIEventMapper(Set.of());
        Assert.assertThrows(InternalNakadiException.class, () -> eventMapper.mapToJsonObject(validKPIEvent));
    }

    @Test
    public void testKPIEventMapperThrowsWhenGetterNotFound() {
        Assert.assertThrows(InternalNakadiException.class,
                () -> new KPIEventMapper(Set.of(KPIEventWithoutGetter.class)));
    }

    @Test
    public void testKPIEventMapperThrowsWhenGetterIsPrivate() {
        Assert.assertThrows(InternalNakadiException.class,
                () -> new KPIEventMapper(Set.of(KPIEventWithPrivateGetter.class)));
    }
}
