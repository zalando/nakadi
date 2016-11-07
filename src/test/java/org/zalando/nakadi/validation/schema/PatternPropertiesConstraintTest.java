package org.zalando.nakadi.validation.schema;

import com.google.common.collect.Lists;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.junit.Test;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class PatternPropertiesConstraintTest {
    @Test
    public void rejectPatternProperty() {
        final SchemaConstraint constraint = new PatternPropertiesConstraint();
        final List<String> jsonPath = Lists.newArrayList("path1", "path2");
        final ObjectSchema schema = mock(ObjectSchema.class);
        final Map<Pattern, Schema> patternProperties = mock(Map.class);

        when(patternProperties.isEmpty()).thenReturn(false);
        when(schema.getPatternProperties()).thenReturn(patternProperties);

        final Optional<SchemaIncompatibility> optionalError = constraint.validate(jsonPath, schema);

        assertThat(optionalError, isPresent());
        assertThat(optionalError.get().getJsonPath(), is("#/path1/path2"));
    }
}
