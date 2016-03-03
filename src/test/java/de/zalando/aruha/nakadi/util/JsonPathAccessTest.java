package de.zalando.aruha.nakadi.util;

import de.zalando.aruha.nakadi.exceptions.InvalidPartitioningKeyFieldsException;
import de.zalando.aruha.nakadi.utils.TestUtils;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class JsonPathAccessTest {

    private static final JSONObject JSON_OBJECT;

    static {
        try {
            JSON_OBJECT = new JSONObject(TestUtils.resourceAsString("JsonPathAccessTest.json", JsonPathAccessTest.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonPathAccess jsonPath = new JsonPathAccess(JSON_OBJECT);

    @Test
    public void canAccessProperties() throws Exception {

        assertThat(jsonPath.get("sku"), equalTo("ABCDE"));
        assertThat(jsonPath.get("brand"), equalTo(JSON_OBJECT.getJSONObject("brand")));
        assertThat(jsonPath.get("brand.name"), equalTo("Superbrand"));
        assertThat(jsonPath.get("dynamic_attributes"), equalTo(JSON_OBJECT.getJSONObject("dynamic_attributes")));
        assertThat(jsonPath.get("'dynamic_attributes'"), equalTo(JSON_OBJECT.getJSONObject("dynamic_attributes")));
        assertThat(jsonPath.get("dynamic_attributes.'field.with.dots'.field'.'\\\\with\\'chars\""), equalTo("you reached it"));

    }

    @Test(expected = InvalidPartitioningKeyFieldsException.class)
    public void throwsExceptionIfPropertyDoesNotExist() throws InvalidPartitioningKeyFieldsException {

        jsonPath.get("does_not_exist");

    }


}