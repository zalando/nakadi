package org.zalando.nakadi.domain;

import org.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zalando.nakadi.domain.StrictJsonParser;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class StrictJsonParserWrongDataTest {
    @Parameterized.Parameter
    public String valueToTest;

    @Parameterized.Parameters
    public static Iterable<String> data() {
        return Arrays.asList(
                "{\"a\":1,}",
                "{,",
                "{\"name",
                "{\"name\"}",
                "{name\"}",
                "{\"name\"NaN}",
                "{\"name\":NaN}"
        );
    }

    @Test(expected = JSONException.class)
    public void testInvalidFormatThrowsException() {
        StrictJsonParser.parseObject(valueToTest);
    }
}
