package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.controller.advice.PostSubscriptionExceptionHandler;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaPartitionEndStatistics;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.SubscriptionService;
import org.zalando.nakadi.service.SubscriptionValidationService;
import org.zalando.nakadi.service.SubscriptionsUriHelper;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.text.MessageFormat.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.AUTO;
import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class SubscriptionControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";
    private static final int PARTITIONS_PER_SUBSCRIPTION = 5;
    private static final Timeline TIMELINE = TestUtils.buildTimelineWithTopic("topic");

    private final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
    private final SubscriptionCache subscriptionCache = mock(SubscriptionCache.class);
    private final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
    private final MockMvc mockMvc;
    private final TopicRepository topicRepository;
    private final ZkSubscriptionClient zkSubscriptionClient;
    private final CursorConverter cursorConverter;
    private final CursorOperationsService cursorOperationsService;
    private final TimelineService timelineService;
    private final SubscriptionValidationService subscriptionValidationService;
    private final EventTypeRepository eventTypeRepository;
    private final TransactionTemplate transactionTemplate;

    public SubscriptionControllerTest() {
        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(Feature.TOKEN_SUBSCRIPTIONS_ITERATION))
                .thenReturn(false);
        when(featureToggleService.isFeatureEnabled(Feature.DISABLE_SUBSCRIPTION_CREATION))
                .thenReturn(false);
        when(featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS))
                .thenReturn(false);

        topicRepository = mock(TopicRepository.class);
        final SubscriptionClientFactory zkSubscriptionClientFactory = mock(SubscriptionClientFactory.class);
        zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClientFactory.createClient(any())).thenReturn(zkSubscriptionClient);
        timelineService = mock(TimelineService.class);
        when(timelineService.getActiveTimeline(any(EventType.class))).thenReturn(TIMELINE);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        final NakadiSettings settings = mock(NakadiSettings.class);
        when(settings.getMaxSubscriptionPartitions()).thenReturn(PARTITIONS_PER_SUBSCRIPTION);
        cursorOperationsService = mock(CursorOperationsService.class);
        cursorConverter = mock(CursorConverter.class);
        subscriptionValidationService = mock(SubscriptionValidationService.class);
        eventTypeRepository = mock(EventTypeRepository.class);
        transactionTemplate = mock(TransactionTemplate .class);
        final NakadiKpiPublisher nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
        final NakadiAuditLogPublisher nakadiAuditLogPublisher = mock(NakadiAuditLogPublisher.class);

        final SubscriptionService subscriptionService = new SubscriptionService(subscriptionRepository,
                subscriptionCache,
                zkSubscriptionClientFactory, timelineService, subscriptionValidationService,
                cursorConverter, cursorOperationsService, nakadiKpiPublisher, featureToggleService, null,
                nakadiAuditLogPublisher, mock(AuthorizationValidator.class), eventTypeCache,
                transactionTemplate, eventTypeRepository);
        final SubscriptionController controller = new SubscriptionController(subscriptionService);
        final ApplicationService applicationService = mock(ApplicationService.class);
        doReturn(true).when(applicationService).exists(any());

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new PostSubscriptionExceptionHandler())
                .setCustomArgumentResolvers(new TestHandlerMethodArgumentResolver())
                .build();
    }

    @Test
    public void whenGetSubscriptionThenOk() throws Exception {
        final Subscription subscription = RandomSubscriptionBuilder.builder().build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        when(subscriptionCache.getSubscription(subscription.getId())).thenReturn(subscription);

        getSubscription(subscription.getId())
                .andExpect(status().isOk())
                .andExpect(content().string(sameJSONAs(TestUtils.OBJECT_MAPPER.writeValueAsString(subscription))));
    }

    @Test
    public void whenGetNoneExistingSubscriptionThenNotFound() throws Exception {
        final Subscription subscription = RandomSubscriptionBuilder.builder().build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        when(subscriptionCache.getSubscription(subscription.getId()))
                .thenThrow(new NoSuchSubscriptionException("dummy-message"));
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "dummy-message");

        getSubscription(subscription.getId())
                .andExpect(status().isNotFound())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListSubscriptionsWithoutQueryParamsThenOk() throws Exception {
        final List<Subscription> subscriptions = TestUtils.createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), any(), any()))
                .thenReturn(subscriptions);
        final PaginationWrapper subscriptionList =
                new PaginationWrapper(subscriptions, new PaginationLinks());

        getSubscriptions()
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1))
                .listSubscriptions(ImmutableSet.of(), Optional.empty(), Optional.ofNullable(null),  Optional.of(
                        new SubscriptionDbRepository.PaginationParameters(20, 0)));
    }

    @Test
    public void whenListSubscriptionsWithQueryParamsThenOk() throws Exception {
        final List<Subscription> subscriptions = TestUtils.createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), any(), any()))
                .thenReturn(subscriptions);
        final PaginationWrapper subscriptionList =
                new PaginationWrapper(subscriptions, new PaginationLinks());

        getSubscriptions(ImmutableSet.of("et1", "et2"), "app", Optional.ofNullable(null), 0, 30)
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(subscriptionList)));

        verify(subscriptionRepository, times(1))
                .listSubscriptions(ImmutableSet.of("et1", "et2"), Optional.of("app"),
                        Optional.ofNullable(null),  Optional.of(new SubscriptionDbRepository.
                                PaginationParameters(30, 0)));
    }

    @Test
    public void whenListSubscriptionsAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionRepository.listSubscriptions(any(), any(), any(), any()))
                .thenThrow(new ServiceTemporarilyUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscriptions(), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsWithNegativeOffsetThenBadRequest() throws Exception {
        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST, "'offset' parameter can't be lower than 0");
        checkForProblem(getSubscriptions(ImmutableSet.of("et"), "app",
                Optional.ofNullable(null), -5, 10), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsWithIncorrectLimitThenBadRequest() throws Exception {
        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST,
                "'limit' parameter should have value between 1 and 1000");
        checkForProblem(getSubscriptions(ImmutableSet.of("et"), "app",
                Optional.ofNullable(null), 0, -5), expectedProblem);
    }

    @Test
    public void whenListSubscriptionsThenPaginationIsOk() throws Exception {
        final List<Subscription> subscriptions = TestUtils.createRandomSubscriptions(10);
        when(subscriptionRepository.listSubscriptions(any(), any(), any(), any()))
                .thenReturn(subscriptions);

        final PaginationLinks.Link prevLink = new PaginationLinks.Link(
                "/subscriptions?event_type=et1&event_type=et2&owning_application=app&offset=0&limit=10");
        final PaginationLinks.Link nextLink = new PaginationLinks.Link(
                "/subscriptions?event_type=et1&event_type=et2&owning_application=app&offset=15&limit=10");
        final PaginationLinks links = new PaginationLinks(Optional.of(prevLink), Optional.of(nextLink));
        final PaginationWrapper expectedResult = new PaginationWrapper(subscriptions, links);

        getSubscriptions(ImmutableSet.of("et1", "et2"), "app", Optional.ofNullable(null), 5, 10)
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedResult)));
    }

    @Test
    public void whenGetSubscriptionAndExceptionThenServiceUnavailable() throws Exception {
        when(subscriptionCache.getSubscription(any()))
                .thenThrow(new ServiceTemporarilyUnavailableException("dummy message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy message");
        checkForProblem(getSubscription("dummyId"), expectedProblem);
    }

    @Test
    public void whenGetSubscriptionStatThenOk() throws Exception {
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withEventType(TIMELINE.getEventType()).build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        final Collection<Partition> partitions = Collections.singleton(
                new Partition(TIMELINE.getEventType(), "0", "xz", null, Partition.State.ASSIGNED));
        final ZkSubscriptionNode zkSubscriptionNode =
                new ZkSubscriptionNode(partitions, Arrays.asList(new Session("xz", 0)));
        when(subscriptionCache.getSubscription(subscription.getId())).thenReturn(subscription);
        when(zkSubscriptionClient.getZkSubscriptionNode()).thenReturn(Optional.of(zkSubscriptionNode));
        final SubscriptionCursorWithoutToken currentOffset =
                new SubscriptionCursorWithoutToken(TIMELINE.getEventType(), "0", "3");
        final EventTypePartition etp = new EventTypePartition(TIMELINE.getEventType(), "0");
        final Map<EventTypePartition, SubscriptionCursorWithoutToken> offsets = new HashMap<>();
        offsets.put(etp, currentOffset);
        when(zkSubscriptionClient.getOffsets(Collections.singleton(etp))).thenReturn(offsets);
        when(eventTypeCache.getEventType(TIMELINE.getEventType()))
                .thenReturn(EventTypeTestBuilder.builder().name(TIMELINE.getEventType()).build());
        final List<PartitionEndStatistics> statistics = Collections.singletonList(
                new KafkaPartitionEndStatistics(TIMELINE, 0, 13));
        when(topicRepository.loadTopicEndStatistics(eq(Collections.singletonList(TIMELINE)))).thenReturn(statistics);
        final NakadiCursor currentCursor = mock(NakadiCursor.class);
        when(currentCursor.getEventTypePartition()).thenReturn(new EventTypePartition(TIMELINE.getEventType(), "0"));
        when(cursorConverter.convert((List<SubscriptionCursorWithoutToken>) any()))
                .thenReturn(Collections.singletonList(currentCursor));
        when(cursorOperationsService.calculateDistance(eq(currentCursor), eq(statistics.get(0).getLast())))
                .thenReturn(10L);

        final List<SubscriptionEventTypeStats> expectedStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        TIMELINE.getEventType(),
                        Collections.singletonList(
                                new SubscriptionEventTypeStats.Partition("0", "assigned", 10L, null, "xz", AUTO)))
                );

        getSubscriptionStats(subscription.getId())
                .andExpect(status().isOk())
                .andExpect(content().string(
                        TestUtils.JSON_TEST_HELPER.matchesObject(new ItemsWrapper<>(expectedStats))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenGetSubscriptionNoEventTypesThenStatEmpty() throws Exception {
        final Subscription subscription = RandomSubscriptionBuilder.builder().withEventType("myET").build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        when(subscriptionCache.getSubscription(subscription.getId())).thenReturn(subscription);
        when(zkSubscriptionClient.getZkSubscriptionNode()).thenReturn(
                Optional.of(new ZkSubscriptionNode(Collections.emptyList(), Collections.emptyList())));
        when(eventTypeCache.getEventType("myET")).thenThrow(NoSuchEventTypeException.class);

        getSubscriptionStats(subscription.getId())
                .andExpect(status().isNotFound());
    }

    private ResultActions getSubscriptionStats(final String subscriptionId) throws Exception {
        return mockMvc.perform(get(format("/subscriptions/{0}/stats", subscriptionId)));
    }

    private ResultActions getSubscriptions() throws Exception {
        return mockMvc.perform(get("/subscriptions"));
    }

    private ResultActions getSubscriptions(final Set<String> eventTypes, final String owningApp,
                                           final Optional<AuthorizationAttribute> reader,
                                           final int offset, final int limit
    ) throws Exception {
        final String url = SubscriptionsUriHelper.createSubscriptionListLink(
                Optional.of(owningApp), eventTypes, reader, offset, Optional.empty(), limit, false).getHref();
        return mockMvc.perform(get(url));
    }

    @Test
    public void whenDeleteSubscriptionThenNoContent() throws Exception {
        mockGetFromRepoSubscriptionWithOwningApp("sid", "nakadiClientId");
        mockMvc.perform(delete("/subscriptions/sid"))
                .andExpect(status().isNoContent());
    }

    @Test
    public void whenDeleteSubscriptionThatDoesNotExistThenNotFound() throws Exception {
        mockGetFromRepoSubscriptionWithOwningApp("sid", "nakadiClientId");

        doThrow(new NoSuchSubscriptionException("dummy message"))
                .when(subscriptionRepository).deleteSubscription("sid");

        checkForProblem(
                mockMvc.perform(delete("/subscriptions/sid")),
                Problem.valueOf(NOT_FOUND, "dummy message"));
    }

    private void mockGetFromRepoSubscriptionWithOwningApp(final String subscriptionId, final String owningApplication)
            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId(subscriptionId)
                .withOwningApplication(owningApplication)
                .build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        when(subscriptionRepository.getSubscription(subscriptionId)).thenReturn(subscription);
    }

    private ResultActions getSubscription(final String subscriptionId) throws Exception {
        return mockMvc.perform(get(format("/subscriptions/{0}", subscriptionId)));
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    private class TestHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

        @Override
        public boolean supportsParameter(final MethodParameter parameter) {
            return true;
        }

        @Override
        public Object resolveArgument(final MethodParameter parameter,
                                      final ModelAndViewContainer mavContainer,
                                      final NativeWebRequest webRequest,
                                      final WebDataBinderFactory binderFactory) {
            return new NakadiClient("nakadiClientId", "");
        }
    }
}
