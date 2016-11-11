package org.zalando.nakadi.validation.schema;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.IsOptional.isAbsent;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class ChangeVersionAndCreatedAtConstraintTest {
    @Test
    public void whenNoChangesKeepVersion() {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder().compatibilityMode(CompatibilityMode.DEPRECATED);

        final EventTypeSchema oldSchema = new EventTypeSchema();
        oldSchema.setSchema("{}");
        oldSchema.setVersion(new Version("1.0.0"));
        oldSchema.setCreatedAt(new DateTime(DateTimeZone.UTC));

        final EventTypeSchema newSchema = new EventTypeSchema();
        newSchema.setSchema("{}");
        newSchema.setVersion(new Version("1.1.0"));
        newSchema.setCreatedAt(oldSchema.getCreatedAt());

        final EventType oldET = builder.schema(oldSchema).build();
        final EventType newET = builder.schema(newSchema).build();
        final SchemaEvolutionConstraint constraint = new ChangeVersionAndCreatedAtConstraint();

        assertThat(constraint.validate(oldET, newET), isPresent());
    }

    @Test
    public void whenNoChangesKeepCreatedAt() {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder().compatibilityMode(CompatibilityMode.DEPRECATED);
        final EventTypeSchema oldSchema = new EventTypeSchema();
        oldSchema.setSchema("{}");
        oldSchema.setVersion(new Version("1.0.0"));
        oldSchema.setCreatedAt(new DateTime(DateTimeZone.UTC));
        final EventTypeSchema newSchema = new EventTypeSchema();
        newSchema.setSchema("{}");
        newSchema.setVersion(new Version("1.0.0"));
        newSchema.setCreatedAt(new DateTime(DateTimeZone.UTC).minus(5));
        final EventType oldET = builder.schema(oldSchema).build();
        final EventType newET = builder.schema(newSchema).build();
        final SchemaEvolutionConstraint constraint = new ChangeVersionAndCreatedAtConstraint();

        assertThat(constraint.validate(oldET, newET), isAbsent());
        assertThat(newET.getSchema().getCreatedAt(), is(equalTo(oldET.getSchema().getCreatedAt())));

    }
}