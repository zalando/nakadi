package org.zalando.nakadi.validation.schema;

import org.junit.Test;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class CategoryChangeConstraintTest {
    @Test
    public void cannotChangeCategory() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.category(EventCategory.BUSINESS).build();
        final EventType newET = builder.category(EventCategory.DATA).build();
        final SchemaEvolutionConstraint constraint = new CategoryChangeConstraint();

        assertThat(constraint.validate(oldET, newET), isPresent());
    }
}