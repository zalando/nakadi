package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.SubscriptionBase;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.Set;

import static de.zalando.aruha.nakadi.utils.TestUtils.invalidProblem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class SubscriptionControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";

    private final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final MockMvc mockMvc;
    private final JsonTestHelper jsonHelper;

    public SubscriptionControllerTest() throws Exception {
        jsonHelper = new JsonTestHelper(objectMapper);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);

        final SubscriptionController controller = new SubscriptionController(subscriptionRepository,
                eventTypeRepository, featureToggleService);
        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
            new MappingJackson2HttpMessageConverter(objectMapper);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .build();
    }

    @Test
    public void whenPostValidSubscriptionThenOk() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription("app", ImmutableSet.of("myET"));
        final Subscription subscription = new Subscription("123", new DateTime(DateTimeZone.UTC), subscriptionBase);
        when(subscriptionRepository.createSubscription(any())).thenReturn(subscription);

        postSubscription(subscriptionBase)
                .andExpect(status().isCreated())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("$.owning_application", equalTo("app")))
                .andExpect(jsonPath("$.event_types", containsInAnyOrder(ImmutableSet.of("myET").toArray())))
                .andExpect(jsonPath("$.consumer_group", equalTo("none")))
                .andExpect(jsonPath("$.created_at", equalTo(subscription.getCreatedAt().toString())))
                .andExpect(jsonPath("$.id", equalTo("123")))
                .andExpect(jsonPath("$.start_from", equalTo("end")));
    }

    @Test
    public void whenOwningApplicationIsNullThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription(null, ImmutableSet.of("myET"));
        final Problem expectedProblem = invalidProblem("owning_application", "may not be null");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenEventTypesIsEmptyThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription("app", ImmutableSet.of());
        final Problem expectedProblem = invalidProblem("event_types", "size must be between 1 and 1");
        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    // this test method will fail when we implement consuming from multiple event types
    public void whenMoreThanOneEventTypeThenUnprocessableEntity() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription("app", ImmutableSet.of("myET", "secondET"));
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
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\"myEt\"],\"start_from\":\"middle\"}";
        postSubscriptionAsJson(subscription).andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenEventTypeDoesNotExistThenNotFound() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription("app", ImmutableSet.of("myET"));
        when(eventTypeRepository.findByName("myET")).thenThrow(new NoSuchEventTypeException(""));

        final Problem expectedProblem = Problem.valueOf(Response.Status.NOT_FOUND,
                "Failed to create subscription, event type(s) not found: 'myET'");

        checkForProblem(postSubscription(subscriptionBase), expectedProblem);
    }

    @Test
    public void whenSubscriptionExistsThenReturnIt() throws Exception {
        final SubscriptionBase subscriptionBase = createSubscription("app", ImmutableSet.of("myET"));
        doThrow(new DuplicatedSubscriptionException("", null)).when(subscriptionRepository).createSubscription(any());

        final Subscription existingSubscription = new Subscription("123", new DateTime(DateTimeZone.UTC), subscriptionBase);
        existingSubscription.setStartFrom(SubscriptionBase.InitialPosition.BEGIN);
        when(subscriptionRepository.getSubscription(eq("app"), eq(ImmutableSet.of("myET")), any()))
                .thenReturn(existingSubscription);

        postSubscription(subscriptionBase)
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string(sameJSONAs(jsonHelper.asJsonString(existingSubscription))));
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    private SubscriptionBase createSubscription(final String owningApplication, final Set<String> eventTypes) {
        final SubscriptionBase subscriptionBase = new SubscriptionBase();
        subscriptionBase.setOwningApplication(owningApplication);
        subscriptionBase.setEventTypes(eventTypes);
        return subscriptionBase;
    }

    private ResultActions postSubscription(final SubscriptionBase subscriptionBase) throws Exception {
        return postSubscriptionAsJson(objectMapper.writeValueAsString(subscriptionBase));
    }

    private ResultActions postSubscriptionAsJson(final String subscription) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions")
                .contentType(APPLICATION_JSON)
                .content(subscription);
        return mockMvc.perform(requestBuilder);
    }

}
