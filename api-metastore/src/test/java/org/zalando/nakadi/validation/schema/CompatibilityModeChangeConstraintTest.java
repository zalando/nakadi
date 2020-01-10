package org.zalando.nakadi.validation.schema;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.IsOptional;

public class CompatibilityModeChangeConstraintTest {
    @Test
    public void cannotDowngradeCompatibilityMode() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint();

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isPresent());
    }

    @Test
    public void canPromoteFromForwardToCompatible() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.FORWARD).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint();

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void canPromoteFromNoneToForward() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.FORWARD).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint();

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void passWhenNoChanges() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint();

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

}