package org.zalando.nakadi.controller;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.core.MethodParameter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.security.AuthorizedClient;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.EventTypeValidator;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.springframework.http.MediaType.APPLICATION_JSON;

public final class EventScopeTest {

    private static final String EVENT_NAME = "my-event";
    private static final String EVENT_BATCH = "[{\"payload\": \"My Event Payload\"}]";
    private static final Set<String> SCOPE_WRITE = Collections.singleton("oauth2.scope.write");
    private static final String URL = "/event-types/" + EVENT_NAME + "/events";

    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final EventTypeCache eventTypeCache;
    private final StandaloneMockMvcBuilder mvcBuilder;

    public EventScopeTest() throws Exception {
        eventTypeCache = Mockito.mock(EventTypeCache.class);
        final EventTypeMetricRegistry eventTypeMetricRegistry = Mockito.mock(EventTypeMetricRegistry.class);
        final EventPublisher publisher = new EventPublisher(Mockito.mock(TopicRepository.class),
                eventTypeCache, Mockito.mock(PartitionResolver.class), Mockito.mock(Enrichment.class));
        final EventPublishingController controller = new EventPublishingController(publisher, eventTypeMetricRegistry);
        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter = new MappingJackson2HttpMessageConverter(objectMapper);

        Mockito.when(eventTypeCache.getValidator(Matchers.any())).thenReturn(new EventTypeValidator(new EventType()));
        Mockito.when(eventTypeMetricRegistry.metricsFor(Matchers.any())).thenReturn(new EventTypeMetrics("", new MetricRegistry()));
        mvcBuilder = MockMvcBuilders.standaloneSetup(controller).setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter);
    }

    @Test
    public void test_scope_write() throws Exception {
        mvcBuilder.setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver(SCOPE_WRITE));
        Mockito.when(eventTypeCache.getEventType(EVENT_NAME)).thenReturn(createEventWith(SCOPE_WRITE));

        publishEvent(EVENT_BATCH).andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void test_no_scope_write() throws Exception {
        mvcBuilder.setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver(Collections.emptySet()));
        Mockito.when(eventTypeCache.getEventType(EVENT_NAME)).thenReturn(createEventWith(SCOPE_WRITE));

        try {
            publishEvent(EVENT_BATCH);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalScopeException);
        }
    }

    private EventType createEventWith(final Set<String> scopes) {
        return EventTypeTestBuilder.builder().writeScope(Optional.ofNullable(scopes)).build();
    }

    private ResultActions publishEvent(final String batch) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(URL)
                .contentType(APPLICATION_JSON)
                .content(batch);

        return mvcBuilder.build().perform(requestBuilder);
    }

    private final class TestHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

        private final Set<String> scopes;

        private TestHandlerMethodArgumentResolver(final Set<String> scopes) {
            this.scopes = scopes;
        }

        @Override
        public boolean supportsParameter(final MethodParameter parameter) {
            return true;
        }

        @Override
        public Client resolveArgument(final MethodParameter parameter,
                                      final ModelAndViewContainer mavContainer,
                                      final NativeWebRequest webRequest,
                                      final WebDataBinderFactory binderFactory) throws Exception {
            return new AuthorizedClient("clientId", scopes);
        }
    }
}
