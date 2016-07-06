package de.zalando.aruha.nakadi.service;

import com.codahale.metrics.MetricRegistry;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClosedConnectionsCrutchTest {

    @Test
    public void tcpFileShouldBeParsedWithoutException() throws IOException {
        final FeatureToggleService fts = mock(FeatureToggleService.class);
        when(fts.isFeatureEnabled(any())).thenReturn(true);

        final ClosedConnectionsCrutch crutch = new ClosedConnectionsCrutch(8080, mock(MetricRegistry.class), fts);

        final Map<ClosedConnectionsCrutch.ConnectionInfo, ClosedConnectionsCrutch.ConnectionState> result =
                crutch.getCurrentConnections(ClosedConnectionsCrutchTest.class.getResourceAsStream("tcp6"));
        Assert.assertEquals(5, result.size());
        Assert.assertTrue(result.containsKey(new ClosedConnectionsCrutch.ConnectionInfo(
                InetAddress.getByName("172.31.9.43"), 21416)));
    }

}