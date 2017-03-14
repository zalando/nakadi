package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.junit.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaPartitionStatistics;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import static java.text.MessageFormat.format;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.util.SubscriptionsUriHelper.createSubscriptionListUri;
import static org.zalando.nakadi.utils.RandomSubscriptionBuilder.builder;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;
import static org.zalando.nakadi.utils.TestUtils.createRandomSubscriptions;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class SubscriptionControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";

    private final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final JsonTestHelper jsonHelper;
    private final StandaloneMockMvcBuilder mockMvcBuilder;
    private final TopicRepository topicRepository;
    private final ZkSubscriptionClient zkSubscriptionClient;
    private static final int PARTITIONS_PER_SUBSCRIPTION = 5;
    private static final Timeline TIMELINE = createFakeTimeline("topic");

    public SubscriptionControllerTest() throws Exception {
        jsonHelper = new JsonTestHelper(objectMapper);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_SUBSCRIPTION_CREATION))
                .thenReturn(false);

        topicRepository = mock(TopicRepository.class);
        final ZkSubscriptionClientFactory zkSubscriptionClientFactory = mock(ZkSubscriptionClientFactory.class);
        zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClient.isSubscriptionCreated()).thenReturn(true);
        when(zkSubscriptionClientFactory.createZkSubscriptionClient(any())).thenReturn(zkSubscriptionClient);
        final TimelineService timelineService = mock(TimelineService.class);
        when(timelineService.getTimeline(any())).thenReturn(TIMELINE);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        final NakadiSettings settings = mock(NakadiSettings.class);
        when(settings.getMaxSubscriptionPartitions()).thenReturn(PARTITIONS_PER_SUBSCRIPTION);
        final SubscriptionService subscriptionService = new SubscriptionService(subscriptionRepository,
                zkSubscriptionClientFactory, timelineService, eventTypeRepository, null);
        final SubscriptionController controller = new SubscriptionController(featureToggleService, subscriptionService);
        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
                new MappingJackson2HttpMessageConverter(objectMapper);
        final ApplicationService applicationService = mock(ApplicationService.class);
        doReturn(true).when(applicationService).exists(any());

        mockMvcBuilder = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .setControllerAdvice(new ExceptionHandling())
                .setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver());
    }

    @Test
    public void whenGetSubscriptionThenOk() throws Exception {
        final Subscription subscription = builder().build();
        when(subscriptionRepository.getSubscription(subscription.getId())).thenReturn(subscription);

        getSubscription(subscription.getId())
                .andExpect(status().isOk())
                .andExpect(content().string(sameJSONAs(objectMapper.writeValueAsString(subscription))));
    }

    @Test
    public void whenGetNoneExistingSubscriptionThenNotFound() throws Exception {
        final Subscription subscription = builder().build();
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
        final PaginationWrapper subscriptionList =
                new PaginationWrapper(subscriptions, new PaginationLinks());

        getSubscriptions()
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1)).listSubscriptions(ImmutableSet.of(), Optional.empty(), 0, 20);
    }

    @Test
    public void whenListSubscriptionsWithQueryParamsThenOk() throws Exception {
        final List<Subscription> subscriptions = createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt())).thenReturn(subscriptions);
        final PaginationWrapper subscriptionList =
                new PaginationWrapper(subscriptions, new PaginationLinks());

        getSubscriptions(ImmutableSet.of("et1", "et2"), "app", 0, 30)
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1))
                .listSubscriptions(ImmutableSet.of("et1", "et2"), Optional.of("app"), 0, 30);
    }

    @Test
    public void whenListSubscriptionsAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt()))
                .thenThrow(new ServiceUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscriptions(), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsWithNegativeOffsetThenBadRequest() throws Exception {
        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST, "'offset' parameter can't be lower than 0");
        checkForProblem(getSubscriptions(ImmutableSet.of("et"), "app", -5, 10), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsWithIncorrectLimitThenBadRequest() throws Exception {
        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST,
                "'limit' parameter should have value from 1 to 1000");
        checkForProblem(getSubscriptions(ImmutableSet.of("et"), "app", 0, -5), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsThenPaginationIsOk() throws Exception {
        final List<Subscription> subscriptions = createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), anyInt(), anyInt())).thenReturn(subscriptions);

        final PaginationLinks.Link prevLink = new PaginationLinks.Link(
                "/subscriptions?event_type=et1&event_type=et2&owning_application=app&offset=0&limit=10");
        final PaginationLinks.Link nextLink = new PaginationLinks.Link(
                "/subscriptions?event_type=et1&event_type=et2&owning_application=app&offset=15&limit=10");
        final PaginationLinks links = new PaginationLinks(Optional.of(prevLink), Optional.of(nextLink));
        final PaginationWrapper expectedResult = new PaginationWrapper(subscriptions, links);

        getSubscriptions(ImmutableSet.of("et1", "et2"), "app", 5, 10)
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(expectedResult)));
    }

    @Test
    public void whenGetSubscriptionAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.getSubscription(any())).thenThrow(new ServiceUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscription("dummyId"), expectedProblem);
    }

    @Test
    public void whenGetSubscriptionStatThenOk() throws Exception {
        final Subscription subscription = builder().withEventType("myET").build();
        final TopicPartition partitionKey = new TopicPartition("topic", "0");
        final Partition[] partitions = {new Partition(partitionKey, "xz", "xz", Partition.State.ASSIGNED)};
        final ZkSubscriptionNode zkSubscriptionNode = new ZkSubscriptionNode();
        zkSubscriptionNode.setPartitions(partitions);
        zkSubscriptionNode.setSessions(new Session[]{new Session("session-is", 0)});
        when(subscriptionRepository.getSubscription(subscription.getId())).thenReturn(subscription);
        when(zkSubscriptionClient.getZkSubscriptionNodeLocked()).thenReturn(zkSubscriptionNode);
        when(zkSubscriptionClient.getOffset(partitionKey)).thenReturn("3");
        when(eventTypeRepository.findByName("myET"))
                .thenReturn(EventTypeTestBuilder.builder().name("myET").topic("topic").build());
        final List<PartitionStatistics> statistics = Collections.singletonList(
                new KafkaPartitionStatistics(TIMELINE, 0, 0, 13));
        when(topicRepository.loadTopicStatistics(eq(Collections.singletonList(TIMELINE)))).thenReturn(statistics);

        final List<SubscriptionEventTypeStats> subscriptionStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        "myET",
                        Collections.singleton(new SubscriptionEventTypeStats.Partition("0", "assigned", 10L, "xz")))
                );

        getSubscriptionStats(subscription.getId())
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(new ItemsWrapper<>(subscriptionStats))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenGetSubscriptionNoEventTypesThenStatEmpty() throws Exception {
        final Subscription subscription = builder().withEventType("myET").build();
        when(subscriptionRepository.getSubscription(subscription.getId())).thenReturn(subscription);
        when(zkSubscriptionClient.getZkSubscriptionNodeLocked()).thenReturn(new ZkSubscriptionNode());
        when(eventTypeRepository.findByName("myET")).thenThrow(NoSuchEventTypeException.class);

        getSubscriptionStats(subscription.getId())
                .andExpect(status().isNotFound());
    }

    private ResultActions getSubscriptionStats(final String subscriptionId) throws Exception {
        return mockMvcBuilder.build().perform(get(format("/subscriptions/{0}/stats", subscriptionId)));
    }

    private ResultActions getSubscriptions() throws Exception {
        return mockMvcBuilder.build().perform(get("/subscriptions"));
    }

    private ResultActions getSubscriptions(final Set<String> eventTypes, final String owningApp, final int offset,
                                           final int limit) throws Exception {
        final String url = createSubscriptionListUri(Optional.of(owningApp), eventTypes, offset, limit);
        return mockMvcBuilder.build().perform(get(url));
    }

    @Test
    public void whenDeleteSubscriptionThenNoContent() throws Exception {
        mockGetFromRepoSubscriptionWithOwningApp("sid", "nakadiClientId");
        mockMvcBuilder.build().perform(delete("/subscriptions/sid"))
                .andExpect(status().isNoContent());
    }

    @Test
    public void whenDeleteSubscriptionThatDoesNotExistThenNotFound() throws Exception {
        mockGetFromRepoSubscriptionWithOwningApp("sid", "nakadiClientId");

        doThrow(new NoSuchSubscriptionException("dummy message"))
                .when(subscriptionRepository).deleteSubscription("sid");

        checkForProblem(
                mockMvcBuilder.build().perform(delete("/subscriptions/sid")),
                Problem.valueOf(NOT_FOUND, "dummy message"));
    }

    @Test
    public void whenDeleteSubscriptionAndOwningAppDoesNotMatchThenForbidden() throws Exception {
        mockGetFromRepoSubscriptionWithOwningApp("sid", "wrongApp");
        checkForProblem(
                mockMvcBuilder.build().perform(delete("/subscriptions/sid")),
                Problem.valueOf(FORBIDDEN, "You don't have access to this subscription"));
    }

    private void mockGetFromRepoSubscriptionWithOwningApp(final String subscriptionId, final String owningApplication)
            throws NoSuchSubscriptionException, ServiceUnavailableException {
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId(subscriptionId)
                .withOwningApplication(owningApplication)
                .build();
        when(subscriptionRepository.getSubscription(subscriptionId)).thenReturn(subscription);
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
