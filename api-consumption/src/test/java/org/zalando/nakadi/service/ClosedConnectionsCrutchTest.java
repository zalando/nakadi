package org.zalando.nakadi.service;

import com.codahale.metrics.MetricRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClosedConnectionsCrutchTest {

    private ClosedConnectionsCrutch crutch;

    @Before
    public void prepareCrutch() {
        final FeatureToggleService fts = mock(FeatureToggleService.class);
        when(fts.isFeatureEnabled(any())).thenReturn(true);

        crutch = new ClosedConnectionsCrutch(8080, mock(MetricRegistry.class), fts);
    }

    @Test
    public void tcp6FileShouldBeParsedWithoutException() throws IOException {
        final Map<ClosedConnectionsCrutch.ConnectionInfo, ClosedConnectionsCrutch.ConnectionState> result =
                crutch.getCurrentConnections(ClosedConnectionsCrutchTest.class.getResourceAsStream("tcp6"));
        Assert.assertEquals(5, result.size());
        Assert.assertTrue(result.containsKey(new ClosedConnectionsCrutch.ConnectionInfo(
                InetAddress.getByName("172.31.9.43"), 21416)));
    }

    @Test
    public void tcpFileShouldBeParsedWithoutException() throws IOException {
        final Map<ClosedConnectionsCrutch.ConnectionInfo, ClosedConnectionsCrutch.ConnectionState> result =
                crutch.getCurrentConnections(ClosedConnectionsCrutchTest.class.getResourceAsStream("tcp"));
        Assert.assertEquals(5, result.size());
        Assert.assertTrue(result.containsKey(new ClosedConnectionsCrutch.ConnectionInfo(
                InetAddress.getByName("127.0.0.1"), 4369)));
    }

}