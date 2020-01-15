package org.zalando.nakadi.domain;

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
        final JSONObject anarchyJson = StrictJsonParser.parseObject(value);
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

    @Test
    public void testNumbers() {
        testSingleString("{\"test\":1e+2}");
        testSingleString("{\"test\":1e-2}");
        testSingleString("{\"test\":1e+2}");
        testSingleString("{\"test\":-1e+4}");
    }
}
