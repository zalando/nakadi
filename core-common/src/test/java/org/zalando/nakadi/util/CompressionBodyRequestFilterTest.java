package org.zalando.nakadi.util;

import com.github.luben.zstd.ZstdInputStream;
import org.eclipse.jetty.server.Request;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class CompressionBodyRequestFilterTest {

    @Test
    public void testDecompressOneZstdEvent() throws Exception {
        final CompressionBodyRequestFilter.FilterServletRequestWrapper wrapper =
                new CompressionBodyRequestFilter.FilterServletRequestWrapper(
                        Mockito.mock(Request.class),
                        new ZstdInputStream(
                                CompressionBodyRequestFilterTest.class.getResourceAsStream("event.zstd"))
                );

        final String json = new String(wrapper.getInputStream().readAllBytes());
        final JSONArray array = new JSONArray(json);

        Assert.assertEquals(1, array.length());
    }
}