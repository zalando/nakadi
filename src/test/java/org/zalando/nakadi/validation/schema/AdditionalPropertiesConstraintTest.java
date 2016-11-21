package org.zalando.nakadi.validation.schema;

import com.google.common.collect.Lists;
import org.everit.json.schema.ObjectSchema;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class AdditionalPropertiesConstraintTest {
    @Test
    public void rejectAdditionalPropertiesSchema() {
        final SchemaConstraint constraint = new AdditionalPropertiesConstraint();
        final List<String> jsonPath = Lists.newArrayList("path1", "path2");
        final ObjectSchema schema = mock(ObjectSchema.class);

        Mockito
                .doReturn(true)
                .when(schema)
                .permitsAdditionalProperties();

        final Optional<SchemaIncompatibility> optionalError = constraint.validate(jsonPath, schema);

        assertThat(optionalError, isPresent());
        assertThat(optionalError.get().getJsonPath(), is("#/path1/path2"));
    }
}