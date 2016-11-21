package org.zalando.nakadi.validation.schema;

import com.google.common.collect.Lists;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.ObjectSchema;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.zalando.nakadi.utils.IsOptional.isPresent;

public class AdditionalItemsConstraintTest {
    @Test
    public void rejectAdditionalPropertiesSchema() {
        final SchemaConstraint constraint = new AdditionalItemsConstraint();
        final List<String> jsonPath = Lists.newArrayList("path1", "path2");
        final ArraySchema schema = mock(ArraySchema.class);

        Mockito
                .doReturn(true)
                .when(schema)
                .permitsAdditionalItems();

        final Optional<SchemaIncompatibility> optionalError = constraint.validate(jsonPath, schema);

        assertThat(optionalError, isPresent());
        assertThat(optionalError.get().getJsonPath(), is("#/path1/path2"));
    }
}