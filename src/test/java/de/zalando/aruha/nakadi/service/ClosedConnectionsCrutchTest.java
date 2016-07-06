package de.zalando.aruha.nakadi.service;

import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class ClosedConnectionsCrutchTest {

    @Test
    public void tcpFileShouldBeParsedWithoutException() throws IOException {
        final ClosedConnectionsCrutch crutch = new ClosedConnectionsCrutch(8080, mock(MetricRegistry.class));

        final Map<ClosedConnectionsCrutch.ConnectionInfo, ClosedConnectionsCrutch.ConnectionState> result =
                crutch.getCurrentConnections(ClosedConnectionsCrutchTest.class.getResourceAsStream("tcp6"));
        Assert.assertEquals(5, result.size());
        Assert.assertTrue(result.containsKey(new ClosedConnectionsCrutch.ConnectionInfo(
                InetAddress.getByName("172.31.9.43"), 21416)));
    }

}