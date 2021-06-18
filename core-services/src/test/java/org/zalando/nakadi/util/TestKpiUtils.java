package org.zalando.nakadi.util;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestKpiUtils {

    @SuppressWarnings("unchecked")
    public static void checkKPIEventSubmitted(final NakadiKpiPublisher nakadiKpiPublisher, final String eventType,
                                              final JSONObject expectedEvent) {
        final ArgumentCaptor<Supplier> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        verify(nakadiKpiPublisher, times(1)).publish(eq(eventType), supplierCaptor.capture());
        assertThat(expectedEvent.similar(supplierCaptor.getValue().get()), is(true));
    }
}
