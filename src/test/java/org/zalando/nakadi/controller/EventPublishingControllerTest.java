package org.zalando.nakadi.controller;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.JsonTestHelper;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.domain.EventPublishingStatus.ABORTED;
import static org.zalando.nakadi.domain.EventPublishingStatus.FAILED;
import static org.zalando.nakadi.domain.EventPublishingStatus.SUBMITTED;
import static org.zalando.nakadi.domain.EventPublishingStep.PARTITIONING;
import static org.zalando.nakadi.domain.EventPublishingStep.PUBLISHING;
import static org.zalando.nakadi.domain.EventPublishingStep.VALIDATING;

public class EventPublishingControllerTest {

    public static final String TOPIC = "my-topic";
    private static final String EVENT_BATCH = "[{\"payload\": \"My Event Payload\"}]";

    private ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private MetricRegistry metricRegistry;
    private JsonTestHelper jsonHelper;
    private EventPublisher publisher;
    private FeatureToggleService featureToggleService;
    private SecuritySettings settings;

    private MockMvc mockMvc;
    private EventTypeMetricRegistry eventTypeMetricRegistry;
    private FloodService floodService;

    @Before
    public void setUp() throws Exception {
        jsonHelper = new JsonTestHelper(objectMapper);
        metricRegistry = new MetricRegistry();
        publisher = mock(EventPublisher.class);
        eventTypeMetricRegistry = new EventTypeMetricRegistry(metricRegistry);
        featureToggleService = mock(FeatureToggleService.class);
        settings = mock(SecuritySettings.class);
        floodService = Mockito.mock(FloodService.class);
        Mockito.when(floodService.isProductionBlocked(any())).thenReturn(false);

        final EventPublishingController controller =
                new EventPublishingController(publisher, eventTypeMetricRegistry, floodService);

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter
                = new MappingJackson2HttpMessageConverter(objectMapper);
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .setCustomArgumentResolvers(new ClientResolver(settings, featureToggleService))
                .build();
    }

    @Test
    public void whenResultIsSubmittedThen200() throws Exception {
        final EventPublishResult result = new EventPublishResult(SUBMITTED, null, null);

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(JSONArray.class), eq(TOPIC), any(Client.class));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isOk())
                .andExpect(content().string(""));
    }

    @Test
    public void whenInvalidPostBodyThen400() throws Exception {
        final String expectedPayload = "{\"type\":\"http://httpstatus.es/400\"," +
                "\"title\":\"Bad Request\",\"status\":400," +
                "\"detail\":\"A JSONArray text must start with '[' at 1 [character 2 line 1]\"}";
        postBatch(TOPIC, "invalid json array").andExpect(status().isBadRequest())
                .andExpect(content().string(expectedPayload));
    }

    @Test
    public void whenResultIsAbortedThen422() throws Exception {
        final EventPublishResult result = new EventPublishResult(ABORTED, PARTITIONING, responses());

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(JSONArray.class), eq(TOPIC), any(Client.class));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(jsonHelper.matchesObject(responses())));
    }

    @Test
    public void whenResultIsAbortedThen207() throws Exception {
        final EventPublishResult result = new EventPublishResult(FAILED, PUBLISHING, responses());

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(JSONArray.class), eq(TOPIC), any(Client.class));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isMultiStatus())
                .andExpect(content().string(jsonHelper.matchesObject(responses())));
    }

    @Test
    public void whenEventTypeNotFoundThen404() throws Exception {
        Mockito
                .doThrow(NoSuchEventTypeException.class)
                .when(publisher)
                .publish(any(JSONArray.class), eq(TOPIC), any(Client.class));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void publishedEventsAreReportedPerEventType() throws Exception {
        final EventPublishResult success = new EventPublishResult(SUBMITTED, null, null);
        Mockito
                .doReturn(success)
                .doReturn(success)
                .doThrow(InternalNakadiException.class)
                .when(publisher)
                .publish(any(), any(), any(Client.class));

        postBatch(TOPIC, EVENT_BATCH);
        postBatch(TOPIC, EVENT_BATCH);
        postBatch(TOPIC, EVENT_BATCH);

        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(TOPIC);

        assertThat(eventTypeMetrics.getResponseCount(200), equalTo(2L));
        assertThat(eventTypeMetrics.getResponseCount(500), equalTo(1L));
    }

    private List<BatchItemResponse> responses() {
        final BatchItemResponse response = new BatchItemResponse();
        response.setPublishingStatus(ABORTED);
        response.setStep(VALIDATING);

        final List<BatchItemResponse> responses = new ArrayList<>();
        responses.add(response);

        return responses;
    }

    private ResultActions postBatch(final String eventType, final String batch) throws Exception {
        final String url = "/event-types/" + eventType + "/events";
        final MockHttpServletRequestBuilder requestBuilder = post(url)
                .contentType(APPLICATION_JSON)
                .content(batch);

        return mockMvc.perform(requestBuilder);
    }
}