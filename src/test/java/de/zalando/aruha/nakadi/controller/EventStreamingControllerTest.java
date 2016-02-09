package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.service.EventStreamConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(EventStreamConfig.class)
public class EventStreamingControllerTest {

    @Test
    public void ggg() throws Exception {
        PowerMockito.whenNew(EventStreamConfig.class).withAnyArguments().thenReturn(null);
    }
}
