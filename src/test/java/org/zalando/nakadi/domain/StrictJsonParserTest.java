package org.zalando.nakadi.domain;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class StrictJsonParserTest {

    private void testSingleString(final String value) {

        final JSONObject orthodoxJson = new JSONObject(value);
        final JSONObject anarchyJson = (JSONObject) StrictJsonParser.parse(value);
        Assert.assertEquals("Checking json " + value, orthodoxJson.toString(), anarchyJson.toString());
    }

    @Test
    public void parse() {
        testSingleString("{\"test\": 1.4e3}");
    }

    @Test
    public void testVeryComplexJson() throws IOException {
        final String veryComplexString;
        try (InputStream veryComplexInput =
                     StrictJsonParserTest.class.getClassLoader().getResourceAsStream("very_complex.json")) {
            veryComplexString = StreamUtils.copyToString(veryComplexInput, Charset.forName("UTF-8"));
        }
        testSingleString(veryComplexString);

    }

    @Test(expected = JSONException.class)
    public void testBadJson() {
        StrictJsonParser.parse("{,");
    }

    @Test(expected = JSONException.class)
    public void testBadJson2() {
        StrictJsonParser.parse("{\"name");
    }

    @Test(expected = JSONException.class)
    public void testBadJson3() {
        StrictJsonParser.parse("{name\"}");
    }

    @Test(expected = JSONException.class)
    public void testBadJson4() {
        StrictJsonParser.parse("{\"name\"NaN}");
    }

    @Test(expected = JSONException.class)
    public void testBadJson5() {
        StrictJsonParser.parse("{\"name\":NaN}");
    }

}