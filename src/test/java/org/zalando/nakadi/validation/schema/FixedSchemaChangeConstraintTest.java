package org.zalando.nakadi.validation.schema;

import org.junit.Test;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class FixedSchemaChangeConstraintTest {
    @Test
    public void failOnChange() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.FIXED)
                .schema("{\"type\": \"string\"}").build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.FIXED)
                .schema("{\"type\": \"number\"}").build();
        final SchemaEvolutionConstraint constraint = new FixedSchemaChangeConstraint();

        assertThat(constraint.validate(oldET, newET), isPresent());
    }
}