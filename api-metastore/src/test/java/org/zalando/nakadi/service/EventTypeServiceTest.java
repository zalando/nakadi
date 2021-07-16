package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.InvalidOwningApplicationException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.db.SubscriptionTokenLister;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.ResourceValidationHelperService;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.util.TestKpiUtils.checkKPIEventSubmitted;

@RunWith(MockitoJUnitRunner.class)
public class EventTypeServiceTest {

    private static final String KPI_ET_LOG_EVENT_TYPE = "et-log";
    protected static final long TOPIC_RETENTION_MIN_MS = 10800000;
    protected static final long TOPIC_RETENTION_MAX_MS = 345600000;

    @Mock
    private Enrichment enrichment;
    @Mock
    private EventTypeRepository eventTypeRepository;
    @Mock
    private EventTypeCache eventTypeCache;
    @Mock
    private FeatureToggleService featureToggleService;
    @Mock
    private NakadiSettings nakadiSettings;
    @Mock
    private PartitionsCalculator partitionsCalculator;
    @Mock
    private PartitionResolver partitionResolver;
    @Mock
    private SchemaEvolutionService schemaEvolutionService;
    @Mock
    private SubscriptionDbRepository subscriptionDbRepository;
    @Mock
    private SubscriptionTokenLister subscriptionTokenLister;
    @Mock
    private TimelineService timelineService;
    @Mock
    private TimelineSync timelineSync;
    @Mock
    private TransactionTemplate transactionTemplate;
    @Mock
    private AuthorizationValidator authorizationValidator;
    @Mock
    private NakadiKpiPublisher nakadiKpiPublisher;
    @Mock
    private NakadiAuditLogPublisher nakadiAuditLogPublisher;
    @Mock
    private AdminService adminService;
    @Mock
    private SchemaService schemaService;
    @Mock
    private ApplicationService applicationService;

    private EventTypeService eventTypeService;

    @Before
    public void setUp() {
        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        final ResourceValidationHelperService validationHelperService = new ResourceValidationHelperService();
        eventTypeService = new EventTypeService(eventTypeRepository, timelineService, partitionResolver, enrichment,
                subscriptionDbRepository, schemaEvolutionService, partitionsCalculator, featureToggleService,
                authorizationValidator, timelineSync, transactionTemplate, nakadiSettings, nakadiKpiPublisher,
                KPI_ET_LOG_EVENT_TYPE, nakadiAuditLogPublisher, eventTypeOptionsValidator, validationHelperService,
                eventTypeCache, schemaService, adminService, subscriptionTokenLister, applicationService);
        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            final TransactionCallback callback = (TransactionCallback) invocation.getArguments()[0];
            return callback.doInTransaction(null);
        });

    }

    @Test
    public void testFailToDeleteEventType() {
        final EventType eventType = TestUtils.buildDefaultEventType();
        doThrow(new InternalNakadiException("Can't delete event type"))
                .when(eventTypeRepository).removeEventType(eventType.getName());
        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());
        final Multimap<TopicRepository, String> topicsToDelete = mock(Multimap.class);

        doReturn(topicsToDelete).when(timelineService).deleteAllTimelinesForEventType(eventType.getName());

        assertThrows(EventTypeDeletionException.class,
                () -> eventTypeService.delete(eventType.getName()));

        // check that topics are not deleted in Kafka
        verifyNoInteractions(topicsToDelete);
    }

    @Test
    public void whenSubscriptionsExistThenCantDeleteEventType() {
        final EventType eventType = TestUtils.buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());
        doReturn(new SubscriptionTokenLister.ListResult(
                ImmutableList.of(RandomSubscriptionBuilder.builder().build()), null, null))
                .when(subscriptionTokenLister)
                .listSubscriptions(
                        ImmutableSet.of(eventType.getName()),
                        Optional.empty(),
                        Optional.empty(),
                        null,
                        20
                );

        when(featureToggleService.isFeatureEnabled(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS))
                .thenReturn(false);

        assertThrows(ConflictException.class,
                () -> eventTypeService.delete(eventType.getName()));
    }

    @Test
    public void testFeatureToggleAllowsDeleteEventTypeWithSubscriptions() {
        final EventType eventType = TestUtils.buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());
        doReturn(new SubscriptionTokenLister.ListResult(
                ImmutableList.of(RandomSubscriptionBuilder.builder().build()), null, null))
                .when(subscriptionTokenLister)
                .listSubscriptions(
                        ImmutableSet.of(eventType.getName()),
                        Optional.empty(),
                        Optional.empty(),
                        null,
                        100
                );

        when(featureToggleService.isFeatureEnabled(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS))
                .thenReturn(true);

        eventTypeService.delete(eventType.getName());
        // no exception should be thrown
    }

    @Test
    public void testFeatureToggleAllowsDeleteEventTypeWithAuthzSectionAndDeletableSubscription() {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.setAuthorization(TestUtils.buildResourceAuthorization());

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());
        doReturn(new SubscriptionTokenLister.ListResult(
                ImmutableList.of(TestUtils.createSubscription("nakadi_archiver", "nakadi_to_s3")), null, null))
                .when(subscriptionTokenLister)
                .listSubscriptions(
                        ImmutableSet.of(eventType.getName()),
                        Optional.empty(),
                        Optional.empty(),
                        null,
                        100
                );

        eventTypeService.delete(eventType.getName());
        // no exception should be thrown
    }

    @Test
    public void testFeatureToggleForbidsDeleteEventTypeWithoutAuthzSection() {
        final EventType eventType = TestUtils.buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());

        when(featureToggleService.isFeatureEnabled(Feature.FORCE_EVENT_TYPE_AUTHZ))
                .thenReturn(true);

        assertThrows(AccessDeniedException.class, () -> eventTypeService.delete(eventType.getName()));
    }

    @Test
    public void testFeatureToggleForbidsDeleteEventTypeWithNonDeletableSubscription() {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.setAuthorization(TestUtils.buildResourceAuthorization());

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());

        doReturn(new SubscriptionTokenLister.ListResult(
                ImmutableList.of(TestUtils.createSubscription("someone", "something")), null, null))
                .when(subscriptionTokenLister)
                .listSubscriptions(
                        ImmutableSet.of(eventType.getName()),
                        Optional.empty(),
                        Optional.empty(),
                        null,
                        20
                );
        when(featureToggleService.isFeatureEnabled(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS))
                .thenReturn(false);

        assertThrows(ConflictException.class,
                () -> eventTypeService.delete(eventType.getName()));
    }

    @Test
    public void testAllowCreatingEventTypeWithInformationalFieldsFromEffectiveSchema() {
        final EventType et = EventTypeTestBuilder.builder()
                .category(EventCategory.DATA)
                .build();
        et.setOrderingKeyFields(Collections.singletonList("metadata.occurred_at"));
        et.setOrderingInstanceIds(Collections.singletonList("metadata.partition"));

        assertDoesNotThrow(() -> eventTypeService.create(et, true));
    }

    @Test
    public void whenCreateEventTypeOwningApplicationValidationFails() {
        final EventType et = EventTypeTestBuilder.builder()
                .category(EventCategory.DATA)
                .build();
        when(applicationService.exists(eq(et.getOwningApplication()))).thenReturn(false);
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)))
                .thenReturn(true);
        assertThrows(InvalidOwningApplicationException.class,
                () -> eventTypeService.create(et, true));
    }

    @Test
    public void whenCreateEventTypeOwningApplicationValidationSucceeds() {
        final EventType et = EventTypeTestBuilder.builder()
                .category(EventCategory.DATA)
                .build();
        when(applicationService.exists(eq(et.getOwningApplication()))).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)))
                .thenReturn(true);
        assertDoesNotThrow(() -> eventTypeService.create(et, true));
    }

    @Test
    public void whenCreateEventTypeOwningApplicationValidationIgnored() {
        final EventType et = EventTypeTestBuilder.builder()
                .category(EventCategory.DATA)
                .build();
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)))
                .thenReturn(false);
        assertDoesNotThrow(() -> eventTypeService.create(et, true));
        verifyNoInteractions(applicationService);
    }


    @Test
    public void shouldRemoveEventTypeWhenTimelineCreationFails() {
        final EventType eventType = TestUtils.buildDefaultEventType();
        when(eventTypeRepository.saveEventType(any())).thenReturn(eventType);
        when(timelineService.createDefaultTimeline(any(), anyInt()))
                .thenThrow(new TopicCreationException("Failed to create topic"));

        assertThrows(TopicCreationException.class, () -> eventTypeService.create(eventType, true));

        verify(eventTypeRepository, times(1)).removeEventType(eventType.getName());
    }

    @Test
    public void whenEventTypeCreatedThenKPIEventSubmitted() {
        final EventType et = TestUtils.buildDefaultEventType();
        eventTypeService.create(et, true);
        checkKPIEventSubmitted(nakadiKpiPublisher, KPI_ET_LOG_EVENT_TYPE,
                new JSONObject()
                        .put("event_type", et.getName())
                        .put("status", "created")
                        .put("category", et.getCategory())
                        .put("authz", "disabled")
                        .put("compatibility_mode", et.getCompatibilityMode()));
    }

    @Test
    public void whenEventTypeUpdatedThenKPIEventSubmitted() {
        final EventType et = TestUtils.buildDefaultEventType();
        when(eventTypeRepository.findByName(et.getName())).thenReturn(et);
        when(schemaEvolutionService.evolve(any(), any())).thenReturn(et);
        eventTypeService.update(et.getName(), et);
        checkKPIEventSubmitted(nakadiKpiPublisher, KPI_ET_LOG_EVENT_TYPE,
                new JSONObject()
                        .put("event_type", et.getName())
                        .put("status", "updated")
                        .put("category", et.getCategory())
                        .put("authz", "disabled")
                        .put("compatibility_mode", et.getCompatibilityMode()));
    }

    @Test
    public void whenEventTypeOwningApplicationUpdatedThenItIsValidated() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType src = builder.build();
        final EventType updated = builder.build();

        when(eventTypeRepository.findByName(src.getName())).thenReturn(src);
        updated.setOwningApplication("Some-unknownApplication");

        when(featureToggleService.isFeatureEnabled(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)).thenReturn(true);
        when(applicationService.exists(eq(updated.getOwningApplication()))).thenReturn(false);

        assertThrows(InvalidOwningApplicationException.class, () -> eventTypeService.update(src.getName(), updated));
    }

    @Test
    public void whenEventTypeOwningApplicationUpdatedThenItIsValidatedIfFTEnabledOnly() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType src = builder.build();
        final EventType updated = builder.build();

        when(eventTypeRepository.findByName(src.getName())).thenReturn(src);
        when(schemaEvolutionService.evolve(any(), any())).thenReturn(src);
        updated.setOwningApplication("Some-unknownApplication");

        when(featureToggleService.isFeatureEnabled(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)).thenReturn(false);

        assertDoesNotThrow(() -> eventTypeService.update(src.getName(), updated));

        verifyNoInteractions(applicationService);
    }

    @Test
    public void whenEventTypeOwningApplicationNotUpdatedThenItIsNotValidated() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType src = builder.build();
        final EventType updated = builder.build();

        when(eventTypeRepository.findByName(src.getName())).thenReturn(src);
        when(schemaEvolutionService.evolve(any(), any())).thenReturn(src);

        when(featureToggleService.isFeatureEnabled(Feature.VALIDATE_EVENT_TYPE_OWNING_APPLICATION)).thenReturn(true);

        assertDoesNotThrow(() -> eventTypeService.update(src.getName(), updated));
        verifyNoInteractions(applicationService);
    }


    @Test
    public void whenEventTypeDeletedThenKPIEventSubmitted() {
        final EventType et = TestUtils.buildDefaultEventType();
        when(eventTypeCache.getEventTypeIfExists(et.getName())).thenReturn(Optional.of(et));

        eventTypeService.delete(et.getName());
        checkKPIEventSubmitted(nakadiKpiPublisher, KPI_ET_LOG_EVENT_TYPE,
                new JSONObject()
                        .put("event_type", et.getName())
                        .put("status", "deleted")
                        .put("category", et.getCategory())
                        .put("authz", "disabled")
                        .put("compatibility_mode", et.getCompatibilityMode()));
    }
}
