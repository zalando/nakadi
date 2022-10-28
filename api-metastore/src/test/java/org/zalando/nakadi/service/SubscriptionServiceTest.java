package org.zalando.nakadi.service;

import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.AuthorizationNotPresentException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.kpi.event.NakadiSubscriptionLog;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionServiceTest {

    private SubscriptionDbRepository subscriptionRepository;
    private SubscriptionCache subscriptionCache;
    private NakadiKpiPublisher nakadiKpiPublisher;
    private SubscriptionService subscriptionService;
    private FeatureToggleService featureToggleService;
    private AuthorizationValidator authorizationValidator;
    private SubscriptionValidationService subscriptionValidationService;
    private EventTypeRepository eventTypeRepository;
    private TransactionTemplate transactionTemplate;

    @Captor
    private ArgumentCaptor<Supplier<SpecificRecord>> subscriptionLogEventCaptor;

    @Before
    public void setUp() {
        final SubscriptionClientFactory zkSubscriptionClientFactory = Mockito.mock(SubscriptionClientFactory.class);
        final ZkSubscriptionClient zkSubscriptionClient = Mockito.mock(ZkSubscriptionClient.class);
        Mockito.when(zkSubscriptionClientFactory.createClient(any())).thenReturn(zkSubscriptionClient);
        final TimelineService timelineService = Mockito.mock(TimelineService.class);
        final CursorOperationsService cursorOperationsService = Mockito.mock(CursorOperationsService.class);
        final CursorConverter cursorConverter = Mockito.mock(CursorConverter.class);
        final EventTypeCache cache = Mockito.mock(EventTypeCache.class);
        final NakadiAuditLogPublisher nakadiAuditLogPublisher = Mockito.mock(NakadiAuditLogPublisher.class);
        subscriptionValidationService = Mockito.mock(SubscriptionValidationService.class);
        nakadiKpiPublisher = Mockito.mock(NakadiKpiPublisher.class);
        subscriptionRepository = Mockito.mock(SubscriptionDbRepository.class);
        subscriptionCache = Mockito.mock(SubscriptionCache.class);
        featureToggleService = Mockito.mock(FeatureToggleService.class);
        authorizationValidator = Mockito.mock(AuthorizationValidator.class);
        eventTypeRepository = Mockito.mock(EventTypeRepository.class);
        transactionTemplate = Mockito.mock(TransactionTemplate.class);

        subscriptionService = new SubscriptionService(subscriptionRepository,
                subscriptionCache, zkSubscriptionClientFactory,
                timelineService, subscriptionValidationService, cursorConverter,
                cursorOperationsService, nakadiKpiPublisher, featureToggleService, null,
                nakadiAuditLogPublisher, authorizationValidator, cache,
                transactionTemplate, eventTypeRepository);
    }

    @Test(expected = AuthorizationNotPresentException.class)
    public void whenFeatureToggleIsOnSubscriptionRequiresAuthorizationOnCreation() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId("my_subscription_id1")
                .build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        Mockito.when(featureToggleService
                .isFeatureEnabled(Feature.FORCE_SUBSCRIPTION_AUTHZ)).thenReturn(true);

        subscriptionService.createSubscription(subscriptionBase);
    }

    @Test(expected = AuthorizationNotPresentException.class)
    public void whenFeatureToggleIsOnSubscriptionRequiresAuthorizationOnUpdation() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId("my_subscription_id1")
                .build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        Mockito.when(featureToggleService
                .isFeatureEnabled(Feature.FORCE_SUBSCRIPTION_AUTHZ)).thenReturn(true);
        subscriptionService.createSubscription(subscriptionBase);
        subscriptionService.updateSubscription("my_subscription_id1", subscription);
    }

    @Test
    public void whenSubscriptionCreatedThenKPIEventSubmitted() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withId("my_subscription_id1")
                .build();
        subscription.setUpdatedAt(subscription.getCreatedAt());
        Mockito.when(transactionTemplate.execute(any())).thenReturn(subscription);
        subscriptionService.createSubscription(subscriptionBase);

        Mockito.verify(nakadiKpiPublisher).publish(subscriptionLogEventCaptor.capture());
        final NakadiSubscriptionLog event = (NakadiSubscriptionLog) subscriptionLogEventCaptor.getValue().get();
        assertEquals("my_subscription_id1", event.getSubscriptionId());
        assertEquals("created", event.getStatus());
    }

    @Test
    public void whenSubscriptionDeletedThenKPIEventSubmitted() {
        Mockito.when(subscriptionRepository.getSubscription(any())).thenReturn(new Subscription());
        subscriptionService.deleteSubscription("my_subscription_id1");

        Mockito.verify(nakadiKpiPublisher).publish(subscriptionLogEventCaptor.capture());
        final NakadiSubscriptionLog event = (NakadiSubscriptionLog) subscriptionLogEventCaptor.getValue().get();
        assertEquals("my_subscription_id1", event.getSubscriptionId());
        assertEquals("deleted", event.getStatus());
    }

    @Test(expected = UnableProcessException.class)
    public void whenSubscriptionCreatedAuthorizationIsValidated() {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();

        Mockito.doThrow(new UnableProcessException("fake"))
                .when(subscriptionValidationService).validateSubscriptionOnCreate(eq(subscriptionBase));

        subscriptionService.createSubscription(subscriptionBase);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenSubscriptionModifiedAuthorizationIsValidated() throws NoSuchSubscriptionException {
        Mockito.doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());

        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .buildSubscriptionBase();

        subscriptionService.updateSubscription("test", subscriptionBase);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenSubscriptionDeletedAuthorizationIsValidated() {
        Mockito.doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());

        subscriptionService.deleteSubscription("test");
    }
}
