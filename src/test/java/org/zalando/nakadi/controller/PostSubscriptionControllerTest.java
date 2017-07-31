package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.problem.Problem;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION;
import static org.zalando.nakadi.utils.RandomSubscriptionBuilder.builder;
import static org.zalando.nakadi.utils.TestUtils.invalidProblem;
import static org.zalando.nakadi.utils.TestUtils.mockAccessDeniedException;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class PostSubscriptionControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";

    private final StandaloneMockMvcBuilder mockMvcBuilder;

    private final ApplicationService applicationService = mock(ApplicationService.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final SubscriptionService subscriptionService = mock(SubscriptionService.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);


    public PostSubscriptionControllerTest() throws Exception {

        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION))
                .thenReturn(false);

        when(applicationService.exists(any())).thenReturn(true);

        when(subscriptionService.getSubscriptionUri(any())).thenCallRealMethod();

        final EventType eventType = mock(EventType.class);
        when(eventTypeRepository.findByNameO(any())).thenReturn(Optional.of(eventType));

        final PostSubscriptionController controller = new PostSubscriptionController(featureToggleService,
                applicationService, subscriptionService, authorizationValidator);

        mockMvcBuilder = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setControllerAdvice(new ExceptionHandling())
                .setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver());
    }

    @Test
    public void whenSubscriptionCreationIsDisabledThenCreationFails() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        when(subscriptionService.getExistingSubscription(any())).thenThrow(new NoSubscriptionException("", null));
        when(featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)).thenReturn(true);

        postSubscription(subscriptionBase).andExpect(status().isServiceUnavailable());
    }

    @Test
    public void whenSubscriptionCreationDisabledThenReturnExistentSubscription() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        final Subscription existingSubscription = new Subscription("123", new DateTime(DateTimeZone.UTC),
                subscriptionBase);
        existingSubscription.setReadFrom(SubscriptionBase.InitialPosition.BEGIN);

        when(subscriptionService.getExistingSubscription(any())).thenReturn(existingSubscription);
        when(featureToggleService.isFeatureEnabled(DISABLE_SUBSCRIPTION_CREATION)).thenReturn(true);

        postSubscription(subscriptionBase)
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string(sameJSONAs(TestUtils.JSON_TEST_HELPER.asJsonString(existingSubscription))))
                .andExpect(header().string("Location", "/subscriptions/123"))
                .andExpect(header().doesNotExist("Content-Location"));
    }

    @Test
    public void whenPostValidSubscriptionThenOk() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        final Subscription subscription = new Subscription("123", new DateTime(DateTimeZone.UTC), subscriptionBase);

        when(subscriptionService.getExistingSubscription(any())).thenThrow(new NoSubscriptionException("", null));
        when(subscriptionService.createSubscription(any(), any())).thenReturn(subscription);

        postSubscription(subscriptionBase)
                .andExpect(status().isCreated())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string(sameJSONAs(TestUtils.JSON_TEST_HELPER.asJsonString(subscription))))
                .andExpect(header().string("Location", "/subscriptions/123"))
                .andExpect(header().string("Content-Location", "/subscriptions/123"));
    }

    @Test
    public void whenCreateSubscriptionWithUnknownApplicationThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        when(applicationService.exists(any())).thenReturn(false);

        postSubscription(subscriptionBase)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }

    @Test
    public void whenCreateSubscriptionWithEmptyConsumerGroupThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder()
                .withConsumerGroup("")
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("consumer_group", "must contain at least one character");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenCreateSubscriptionWithEmptyOwningApplicationThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder()
                .withOwningApplication("")
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("owning_application", "must contain at least one character");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenOwningApplicationIsNullThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder()
                .withOwningApplication(null)
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("owning_application", "may not be null");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenEventTypesIsEmptyThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder()
                .withEventTypes(ImmutableSet.of())
                .buildSubscriptionBase();
        final Problem expectedProblem = invalidProblem("event_types", "must contain at least one element");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenMoreThanAllowedEventTypeThenUnprocessableEntity() throws Exception {
        when(subscriptionService.getExistingSubscription(any())).thenThrow(new NoSubscriptionException("", null));
        when(subscriptionService.createSubscription(any(), any())).thenThrow(new TooManyPartitionsException("msg"));
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "msg");
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
                "{\"owning_application\":\"app\",\"event_types\":[\"myEt\"],\"read_from\":\"middle\"}";
        postSubscriptionAsJson(subscription)
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenEventTypeDoesNotExistThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        when(subscriptionService.getExistingSubscription(any())).thenThrow(new NoSubscriptionException("", null));
        when(subscriptionService.createSubscription(any(), any())).thenThrow(new NoEventTypeException("msg"));

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "msg");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenSubscriptionExistsThenReturnIt() throws Exception {
        final SubscriptionBase subscriptionBase = builder().buildSubscriptionBase();
        final Subscription existingSubscription = new Subscription("123", new DateTime(DateTimeZone.UTC),
                subscriptionBase);

        when(subscriptionService.getExistingSubscription(any())).thenReturn(existingSubscription);
        when(subscriptionService.createSubscription(any(), any())).thenThrow(new NoEventTypeException("msg"));

        postSubscription(subscriptionBase)
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string(sameJSONAs(TestUtils.JSON_TEST_HELPER.asJsonString(existingSubscription))))
                .andExpect(header().string("Location", "/subscriptions/123"))
                .andExpect(header().doesNotExist("Content-Location"));
    }

    @Test
    public void whenPostSubscriptionWithNoReadScopeThenForbidden() throws Exception {
        when(subscriptionService.getExistingSubscription(any())).thenThrow(new NoSubscriptionException("", null));
        when(subscriptionService.createSubscription(any(), any()))
                .thenThrow(new IllegalScopeException(ImmutableSet.of("dummyScope")));

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Client has to have scopes: [dummyScope]");
        checkForProblem(postSubscription(builder().buildSubscriptionBase()), expectedProblem);
    }

    @Test
    public void whenEventTypeIsNotAuthorizedThenForbidden() throws Exception {
        final Subscription subscription = mock(Subscription.class);
        when(subscription.getEventTypes()).thenReturn(Sets.newHashSet("event-type-name"));
        when(eventTypeRepository.findByNameO(any())).thenReturn(Optional.of(mock(EventType.class)));

        Mockito.doThrow(mockAccessDeniedException()).when(authorizationValidator)
                .authorizeSubscriptionRead(any());

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Access on READ some-type:some-name denied");
        checkForProblem(postSubscription(builder().buildSubscriptionBase()), expectedProblem);
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    private ResultActions postSubscription(final SubscriptionBase subscriptionBase) throws Exception {
        return postSubscriptionAsJson(TestUtils.OBJECT_MAPPER.writeValueAsString(subscriptionBase));
    }

    private ResultActions postSubscriptionAsJson(final String subscription) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions")
                .contentType(APPLICATION_JSON)
                .content(subscription);
        return mockMvcBuilder.build().perform(requestBuilder);
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
