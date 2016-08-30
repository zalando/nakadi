package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionListWrapper;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.utils.RandomSubscriptionBuilder.randomSubscription;
import static org.zalando.nakadi.utils.TestUtils.createRandomSubscriptions;
import static org.zalando.nakadi.utils.TestUtils.invalidProblem;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class SubscriptionControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";

    private final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final JsonTestHelper jsonHelper;
    private final StandaloneMockMvcBuilder mockMvcBuilder;
    private final ApplicationService applicationService = mock(ApplicationService.class);

    public SubscriptionControllerTest() throws Exception {
        jsonHelper = new JsonTestHelper(objectMapper);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);

        final SubscriptionController controller = new SubscriptionController(subscriptionRepository,
                eventTypeRepository, featureToggleService, applicationService);
        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
                new MappingJackson2HttpMessageConverter(objectMapper);
        doReturn(true).when(applicationService).exists(any());

        mockMvcBuilder = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .setControllerAdvice(new ExceptionHandling())
                .setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver());
    }

    @Test
    public void whenPostValidSubscriptionThenOk() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        final Subscription subscription = new Subscription("123", new DateTime(DateTimeZone.UTC), subscriptionBase);
        when(subscriptionRepository.createSubscription(any())).thenReturn(subscription);
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getOptionalEventType());

        postSubscription(subscriptionBase)
                .andExpect(status().isCreated())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("$.owning_application", equalTo("app")))
                .andExpect(jsonPath("$.event_types", containsInAnyOrder(ImmutableSet.of("myET").toArray())))
                .andExpect(jsonPath("$.consumer_group", equalTo(subscription.getConsumerGroup())))
                .andExpect(jsonPath("$.created_at", equalTo(subscription.getCreatedAt().toString())))
                .andExpect(jsonPath("$.id", equalTo("123")))
                .andExpect(jsonPath("$.start_from", equalTo("end")));
    }

    @Test
    public void whenCreateSubscriptionWithUnknownApplicationThen422() throws Exception {

        doReturn(false).when(applicationService).exists(any());
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        final Subscription subscription = new Subscription("123", new DateTime(DateTimeZone.UTC), subscriptionBase);
        when(subscriptionRepository.createSubscription(any())).thenReturn(subscription);
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getOptionalEventType());

        postSubscription(subscriptionBase)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }

    @Test
    public void whenOwningApplicationIsNullThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication(null)
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("owning_application", "may not be null");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenEventTypesIsEmptyThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of())
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("event_types", "size must be between 1 and 1");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    // this test method will fail when we implement consuming from multiple event types
    public void whenMoreThanOneEventTypeThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET", "secondET"))
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("event_types", "size must be between 1 and 1");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenEventTypesIsNullThenUnprocessableEntity() throws Exception {
        final String subscription = "{\"owning_application\":\"app\",\"consumer_group\":\"myGroup\"}";
        final Problem expectedProblem = invalidProblem("event_types", "may not be null");
        checkForProblem(postSubscriptionAsJson(subscription), expectedProblem);
    }

    @Test
    public void whenWrongStartFromThenBadRequest() throws Exception {
        final String subscription =
                "{\"owning_application\":\"app\",\"event_types\":[\"myEt\"],\"start_from\":\"middle\"}";
        postSubscriptionAsJson(subscription).andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenEventTypeDoesNotExistThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        when(eventTypeRepository.findByNameO("myET")).thenReturn(Optional.empty());

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "Failed to create subscription, event type(s) not found: 'myET'");

        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenSubscriptionExistsThenReturnIt() throws Exception {
        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        doThrow(new DuplicatedSubscriptionException("", null)).when(subscriptionRepository).createSubscription(any());

        final Subscription existingSubscription = new Subscription("123", new DateTime(DateTimeZone.UTC),
                subscriptionBase);
        existingSubscription.setStartFrom(SubscriptionBase.InitialPosition.BEGIN);
        when(subscriptionRepository.getSubscription(eq("app"), eq(ImmutableSet.of("myET")), any()))
                .thenReturn(existingSubscription);
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getOptionalEventType());

        postSubscription(subscriptionBase)
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string(sameJSONAs(jsonHelper.asJsonString(existingSubscription))));
    }

    @Test
    public void whenGetSubscriptionThenOk() throws Exception {
        final Subscription subscription = randomSubscription().build();
        when(subscriptionRepository.getSubscription(subscription.getId())).thenReturn(subscription);

        getSubscription(subscription.getId())
                .andExpect(status().isOk())
                .andExpect(content().string(sameJSONAs(objectMapper.writeValueAsString(subscription))));
    }

    @Test
    public void whenGetNoneExistingSubscriptionThenNotFound() throws Exception {
        final Subscription subscription = randomSubscription().build();
        when(subscriptionRepository.getSubscription(subscription.getId()))
                .thenThrow(new NoSuchSubscriptionException("dummy-message"));
        final ThrowableProblem expectedProblem = Problem.valueOf(Response.Status.NOT_FOUND, "dummy-message");

        getSubscription(subscription.getId())
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListSubscriptionsWithoutQueryParamsThenOk() throws Exception {
        final List<Subscription> subscriptions = createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt())).thenReturn(subscriptions);
        final SubscriptionListWrapper subscriptionList =
                new SubscriptionListWrapper(subscriptions, new PaginationLinks());

        getSubscriptions()
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1)).listSubscriptions(ImmutableSet.of(), Optional.empty(), 0, 20);
    }

    @Test
    public void whenListSubscriptionsWithQueryParamsThenOk() throws Exception {
        final List<Subscription> subscriptions = createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt())).thenReturn(subscriptions);
        final SubscriptionListWrapper subscriptionList =
                new SubscriptionListWrapper(subscriptions, new PaginationLinks());

        getSubscriptions(ImmutableSet.of("et1", "et2"), "app", 10, 30)
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1))
                .listSubscriptions(ImmutableSet.of("et1", "et2"), Optional.of("app"), 10, 30);
    }

    @Test
    public void whenListSubscriptionsAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt()))
                .thenThrow(new ServiceUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscriptions(), expectedProblem);
    }

    @Test
    public void whenGetSubscriptionAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.getSubscription(any())).thenThrow(new ServiceUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscription("dummyId"), expectedProblem);
    }

    @Test
    public void whenPostSubscriptionAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.createSubscription(any()))
                .thenThrow(new ServiceUnavailableException("dummy message"));
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getOptionalEventType());
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        final SubscriptionBase subscription = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        checkForProblem(postSubscription(subscription), expectedProblem);
    }

    private ResultActions getSubscriptions() throws Exception {
        return mockMvcBuilder.build().perform(get("/subscriptions"));
    }

    private ResultActions getSubscriptions(final Set<String> eventTypes, final String owningApp, final int offset,
                                           final int limit) throws Exception {
        final String etQuery = eventTypes.stream()
                .map(et -> format("event_type={0}", et))
                .collect(Collectors.joining("&"));

        final String url = format("/subscriptions?owning_application={0}&{1}&offset={2}&limit={3}",
                owningApp, etQuery, offset, limit);
        return mockMvcBuilder.build().perform(get(url));
    }

    @Test
    public void whenPostSubscriptionWithNoReadScopeThenForbidden() throws Exception {
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getEventTypeWithReadScope());

        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Client has to have scopes: [oauth.read.scope]");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenPostSubscriptionWithReadScopeThenCreated() throws Exception {
        when(eventTypeRepository.findByNameO("myET")).thenReturn(getEventTypeWithReadScope());

        final SubscriptionBase subscriptionBase = randomSubscription()
                .withOwningApplication("app")
                .withEventTypes(ImmutableSet.of("myET"))
                .buildSubscriptionBase();
        postSubscriptionWithScope(subscriptionBase,  Collections.singleton("oauth.read.scope"))
                .andExpect(status().isCreated());
    }

    private Optional<EventType> getEventTypeWithReadScope() {
        return Optional.of(EventTypeTestBuilder.builder()
                .name("myET")
                .readScopes(Collections.singleton("oauth.read.scope"))
                .build());
    }

    private ResultActions getSubscription(final String subscriptionId) throws Exception {
        return mockMvcBuilder.build().perform(get(format("/subscriptions/{0}", subscriptionId)));
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    private ResultActions postSubscription(final SubscriptionBase subscriptionBase) throws Exception {
        return postSubscriptionAsJson(objectMapper.writeValueAsString(subscriptionBase));
    }

    private ResultActions postSubscriptionAsJson(final String subscription) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions")
                .contentType(APPLICATION_JSON)
                .content(subscription);
        return mockMvcBuilder.build().perform(requestBuilder);
    }

    private ResultActions postSubscriptionWithScope(final SubscriptionBase subscriptionBase, final Set<String> scopes)
            throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(subscriptionBase));
        return mockMvcBuilder
                .setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver().addScope(scopes))
                .build()
                .perform(requestBuilder);
    }

    private Optional<EventType> getOptionalEventType() {
        return Optional.of(EventTypeTestBuilder.builder().name("myET").build());
    }

    private class TestHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

        private Set<String> scopes = new HashSet<>();

        public TestHandlerMethodArgumentResolver addScope(final Set<String> scopes) {
            this.scopes = scopes;
            return this;
        }

        @Override
        public boolean supportsParameter(final MethodParameter parameter) {
            return true;
        }

        @Override
        public Object resolveArgument(final MethodParameter parameter,
                                      final ModelAndViewContainer mavContainer,
                                      final NativeWebRequest webRequest,
                                      final WebDataBinderFactory binderFactory) throws Exception {
            return new NakadiClient("nakadiClientId", scopes);
        }
    }
}
