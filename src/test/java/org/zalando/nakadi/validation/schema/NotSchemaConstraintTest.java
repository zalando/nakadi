package org.zalando.nakadi.validation.schema;

import com.google.common.collect.Lists;
import org.everit.json.schema.NotSchema;
import org.junit.Test;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class NotSchemaConstraintTest {
    @Test
    public void rejectNotSchema() {
        final SchemaConstraint constraint = new NotSchemaConstraint();
        final List<String> jsonPath = Lists.newArrayList("path1", "path2");
        final NotSchema schema = mock(NotSchema.class);
        final Optional<SchemaIncompatibility> optionalError = constraint.validate(jsonPath, schema);

        assertThat(optionalError, isPresent());
        assertThat(optionalError.get().getJsonPath(), is("#/path1/path2"));
    }
}
