package org.zalando.nakadi.controller;

import com.codahale.metrics.MetricRegistry;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.EventPublishingExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.NakadiKpiPublisher;
import org.zalando.nakadi.utils.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;
import static org.zalando.nakadi.domain.EventPublishingStatus.ABORTED;
import static org.zalando.nakadi.domain.EventPublishingStatus.FAILED;
import static org.zalando.nakadi.domain.EventPublishingStatus.SUBMITTED;
import static org.zalando.nakadi.domain.EventPublishingStep.PARTITIONING;
import static org.zalando.nakadi.domain.EventPublishingStep.PUBLISHING;
import static org.zalando.nakadi.domain.EventPublishingStep.VALIDATING;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

public class EventPublishingControllerTest {

    public static final String TOPIC = "my-topic";
    private static final String EVENT_BATCH = "[{\"payload\": \"My Event Payload\"}]";

    private MetricRegistry metricRegistry;
    private EventPublisher publisher;
    private SecuritySettings settings;

    private MockMvc mockMvc;
    private EventTypeMetricRegistry eventTypeMetricRegistry;
    private NakadiKpiPublisher kpiPublisher;
    private BlacklistService blacklistService;
    private AuthorizationService authorizationService;
    private EventTypeService eventTypeService;
    private AuthorizationValidator authorizationValidator;

    @Before
    public void setUp() {
        metricRegistry = new MetricRegistry();
        publisher = mock(EventPublisher.class);
        eventTypeMetricRegistry = new EventTypeMetricRegistry(metricRegistry);
        kpiPublisher = mock(NakadiKpiPublisher.class);
        settings = mock(SecuritySettings.class);
        authorizationService = mock(AuthorizationService.class);
        eventTypeService = mock(EventTypeService.class);
        authorizationValidator = mock(AuthorizationValidator.class);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() ->  "adminClientId"));
        when(settings.getAuthMode()).thenReturn(OFF);
        when(settings.getAdminClientId()).thenReturn("adminClientId");

        blacklistService = Mockito.mock(BlacklistService.class);
        when(blacklistService.isProductionBlocked(any(), any())).thenReturn(false);

        final EventPublishingController controller =
                new EventPublishingController(publisher, eventTypeMetricRegistry, blacklistService, kpiPublisher,
                        "kpiEventTypeName", authorizationValidator, eventTypeService);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new EventPublishingExceptionHandler())
                .build();
    }

    @Test
    public void whenResultIsSubmittedThen200() throws Exception {
        final EventPublishResult result = new EventPublishResult(SUBMITTED, null, submittedResponses(1));

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(String.class), eq(TOPIC));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isOk())
                .andExpect(content().string(""));
    }

    @Test
    public void whenInvalidPostBodyThen400() throws Exception {

        Mockito.doThrow(new JSONException("Error"))
                .when(publisher)
                .publish(any(String.class), eq(TOPIC));

        postBatch(TOPIC, "invalid json array").andExpect(status().isBadRequest());
    }

    @Test
    public void whenEventPublishTimeoutThen503() throws Exception {
        when(publisher.publish(any(), any())).thenThrow(new EventTypeTimeoutException(""));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    public void whenResultIsAbortedThen422() throws Exception {
        final EventPublishResult result = new EventPublishResult(ABORTED, PARTITIONING, responses());

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(String.class), eq(TOPIC));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(responses())));
    }

    @Test
    public void whenResultIsAbortedThen207() throws Exception {
        final EventPublishResult result = new EventPublishResult(FAILED, PUBLISHING, responses());

        Mockito
                .doReturn(result)
                .when(publisher)
                .publish(any(String.class), eq(TOPIC));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(status().isMultiStatus())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(responses())));
    }

    @Test
    public void whenEventTypeNotFoundThen404() throws Exception {
        Mockito
                .doThrow(new NoSuchEventTypeException("topic not found"))
                .when(publisher)
                .publish(any(String.class), eq(TOPIC));

        postBatch(TOPIC, EVENT_BATCH)
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void publishedEventsAreReportedPerEventType() throws Exception {
        final EventPublishResult success = new EventPublishResult(SUBMITTED, null, submittedResponses(3));
        Mockito
                .doReturn(success)
                .doReturn(success)
                .doThrow(InternalNakadiException.class)
                .when(publisher)
                .publish(any(), any());

        postBatch(TOPIC, EVENT_BATCH);
        postBatch(TOPIC, EVENT_BATCH);
        postBatch(TOPIC, EVENT_BATCH);

        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(TOPIC);

        assertThat(eventTypeMetrics.getResponseCount(200), equalTo(2L));
        assertThat(eventTypeMetrics.getResponseCount(500), equalTo(1L));
    }

    @Test
    public void publishedEventsKPIReported() throws Exception {
        final EventPublishResult success = new EventPublishResult(SUBMITTED, null, submittedResponses(3));
        Mockito
                .doReturn(success)
                .doReturn(success)
                .doThrow(InternalNakadiException.class)
                .when(publisher)
                .publish(any(), any());

        when(kpiPublisher.hash(any())).thenReturn("hashed-application-name");

        postBatch(TOPIC, EVENT_BATCH);

        final ArgumentCaptor<String> etNameCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<Supplier> eventGeneratorCaptor = ArgumentCaptor.forClass(Supplier.class);

        verify(kpiPublisher, times(1)).publish(etNameCaptor.capture(),
                eventGeneratorCaptor.capture());

        assertThat(etNameCaptor.getValue(), equalTo("kpiEventTypeName"));

        final JSONObject kpi = (JSONObject) eventGeneratorCaptor.getValue().get();
        assertThat(kpi,
                is(sameJSONObjectAs(new JSONObject().put("app", "adminClientId")
                        .put("app_hashed", "hashed-application-name")
                        .put("event_type", "my-topic")
                        .put("batch_size", 33)
                        .put("number_of_events", 3)).allowingExtraUnexpectedFields()));

        assertThat(kpi.getInt("ms_spent"), is(notNullValue()));
    }

    private List<BatchItemResponse> responses() {
        final BatchItemResponse response = new BatchItemResponse();
        response.setPublishingStatus(ABORTED);
        response.setStep(VALIDATING);

        final List<BatchItemResponse> responses = new ArrayList<>();
        responses.add(response);

        return responses;
    }

    private List<BatchItemResponse> submittedResponses(final int number) {
        return responses(number, SUBMITTED, PUBLISHING);
    }

    private List<BatchItemResponse> responses(final int number, final EventPublishingStatus status,
                                              final EventPublishingStep step) {
        final List<BatchItemResponse> responses = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            final BatchItemResponse response = new BatchItemResponse();
            response.setPublishingStatus(status);
            response.setStep(step);
            responses.add(response);
        }
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
