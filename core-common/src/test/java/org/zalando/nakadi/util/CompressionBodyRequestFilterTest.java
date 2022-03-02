package org.zalando.nakadi.util;

import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

public class CompressionBodyRequestFilterTest {

    @Test
    public void testDecompressOneZstdEvent() throws Exception {
        final CompressionBodyRequestFilter.CompressionServletInputStream stream =
                new CompressionBodyRequestFilter.CompressionServletInputStream(
                        CompressionBodyRequestFilterTest.class.getResourceAsStream("event.zstd"));

        final String json = new String(stream.readAllBytes());
        System.out.println(json);
        final JSONArray array = new JSONArray(json);

        Assert.assertEquals(1, array.length());
    }
}