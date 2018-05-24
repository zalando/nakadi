package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.service.subscription.SubscriptionValidationService;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.checkKPIEventSubmitted;

public class SubscriptionServiceTest {

    private static final String SUBSCRIPTION_LOG_ET = "subscription_log_et";

    private final SubscriptionDbRepository subscriptionRepository;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final SubscriptionService subscriptionService;
    private final FeatureToggleService featureToggleService;

    public SubscriptionServiceTest() throws Exception {
        final SubscriptionClientFactory zkSubscriptionClientFactory = mock(SubscriptionClientFactory.class);
        final ZkSubscriptionClient zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClientFactory.createClient(any(), any())).thenReturn(zkSubscriptionClient);
        final TimelineService timelineService = mock(TimelineService.class);
        final CursorOperationsService cursorOperationsService = mock(CursorOperationsService.class);
        final CursorConverter cursorConverter = mock(CursorConverter.class);
        final SubscriptionValidationService subscriptionValidationService = mock(SubscriptionValidationService.class);
        final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
        nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
        subscriptionRepository = mock(SubscriptionDbRepository.class);
        featureToggleService = mock(FeatureToggleService.class);

        subscriptionService = new SubscriptionService(subscriptionRepository, zkSubscriptionClientFactory,
                timelineService, eventTypeRepository, subscriptionValidationService, cursorConverter,
                cursorOperationsService, nakadiKpiPublisher, featureToggleService, SUBSCRIPTION_LOG_ET);
    }

    @Test
    public void whenSubscriptionCreatedThenKPIEventSubmitted() throws Exception {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId("my_subscription_id1")
                .build();
        when(subscriptionRepository.createSubscription(subscriptionBase)).thenReturn(subscription);

        subscriptionService.createSubscription(subscriptionBase);

        checkKPIEventSubmitted(nakadiKpiPublisher, SUBSCRIPTION_LOG_ET,
                new JSONObject()
                        .put("subscription_id", "my_subscription_id1")
                        .put("status", "created"));
    }

    @Test
    public void whenSubscriptionDeletedThenKPIEventSubmitted() throws Exception {
        subscriptionService.deleteSubscription("my_subscription_id1");

        checkKPIEventSubmitted(nakadiKpiPublisher, SUBSCRIPTION_LOG_ET,
                new JSONObject()
                        .put("subscription_id", "my_subscription_id1")
                        .put("status", "deleted"));
    }

}
