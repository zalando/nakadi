package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.SubscriptionService;
import org.zalando.nakadi.service.subscription.SubscriptionValidationService;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.checkKPIEventSubmitted;

public class SubscriptionServiceTest {

    private static final String SUBSCRIPTION_LOG_ET = "subscription_log_et";

    private SubscriptionDbRepository subscriptionRepository;
    private NakadiKpiPublisher nakadiKpiPublisher;
    private SubscriptionService subscriptionService;
    private FeatureToggleService featureToggleService;
    private AuthorizationValidator authorizationValidator;
    private SubscriptionValidationService subscriptionValidationService;
    private AuthorizationService authorizationService;

    @Before
    public void setUp() throws Exception {
        final SubscriptionClientFactory zkSubscriptionClientFactory = mock(SubscriptionClientFactory.class);
        final ZkSubscriptionClient zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClientFactory.createClient(any(), any())).thenReturn(zkSubscriptionClient);
        final TimelineService timelineService = mock(TimelineService.class);
        final CursorOperationsService cursorOperationsService = mock(CursorOperationsService.class);
        final CursorConverter cursorConverter = mock(CursorConverter.class);
        final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
        subscriptionValidationService = mock(SubscriptionValidationService.class);
        nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
        subscriptionRepository = mock(SubscriptionDbRepository.class);
        featureToggleService = mock(FeatureToggleService.class);
        authorizationValidator = mock(AuthorizationValidator.class);
        authorizationService = mock(AuthorizationService.class);
        when(authorizationService.filter(any())).thenAnswer(i -> i.getArguments()[0]);

        subscriptionService = new SubscriptionService(subscriptionRepository, zkSubscriptionClientFactory,
                timelineService, eventTypeRepository, subscriptionValidationService, cursorConverter,
                cursorOperationsService, nakadiKpiPublisher, featureToggleService, null, SUBSCRIPTION_LOG_ET,
                authorizationValidator, authorizationService);
    }

    @Test
    public void whenSubscriptionCreatedThenKPIEventSubmitted() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId("my_subscription_id1")
                .build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        when(subscriptionRepository.createSubscription(subscriptionBase)).thenReturn(subscription);

        subscriptionService.createSubscription(subscriptionBase);

        checkKPIEventSubmitted(nakadiKpiPublisher, SUBSCRIPTION_LOG_ET,
                new JSONObject()
                        .put("subscription_id", "my_subscription_id1")
                        .put("status", "created"));
    }

    @Test
    public void whenSubscriptionDeletedThenKPIEventSubmitted() {
        subscriptionService.deleteSubscription("my_subscription_id1");

        checkKPIEventSubmitted(nakadiKpiPublisher, SUBSCRIPTION_LOG_ET,
                new JSONObject()
                        .put("subscription_id", "my_subscription_id1")
                        .put("status", "deleted"));
    }

    @Test(expected = UnableProcessException.class)
    public void whenSubscriptionCreatedAuthorizationIsValidated() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();

        doThrow(new UnableProcessException("fake"))
                .when(subscriptionValidationService).validateSubscription(eq(subscriptionBase));

        subscriptionService.createSubscription(subscriptionBase);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenSubscriptionModifiedAuthorizationIsValidated() throws NoSuchSubscriptionException {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());

        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();

        subscriptionService.updateSubscription("test", subscriptionBase);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenSubscriptionDeletedAuthorizationIsValidated() {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());

        subscriptionService.deleteSubscription("test");
    }
}
