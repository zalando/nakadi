package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.assertj.core.util.Lists;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.validation.SchemaEvolutionService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.buildResourceAuthorization;
import static org.zalando.nakadi.utils.TestUtils.checkKPIEventSubmitted;
import static org.zalando.nakadi.utils.TestUtils.createSubscription;

public class EventTypeServiceTest {

    private static final String KPI_ET_LOG_EVENT_TYPE = "et-log";
    protected static final long TOPIC_RETENTION_MIN_MS = 10800000;
    protected static final long TOPIC_RETENTION_MAX_MS = 345600000;

    private final Enrichment enrichment = mock(Enrichment.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
    private final PartitionsCalculator partitionsCalculator = mock(PartitionsCalculator.class);
    private final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    private final SchemaEvolutionService schemaEvolutionService = mock(SchemaEvolutionService.class);
    private final SubscriptionDbRepository subscriptionDbRepository = mock(SubscriptionDbRepository.class);
    private final TimelineService timelineService = mock(TimelineService.class);
    private final TimelineSync timelineSync = mock(TimelineSync.class);
    private final TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    private final NakadiKpiPublisher nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
    private final NakadiAuditLogPublisher nakadiAuditLogPublisher = mock(NakadiAuditLogPublisher.class);
    private final AdminService adminService = mock(AdminService.class);
    private EventTypeService eventTypeService;

    @Before
    public void setUp() {
        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        eventTypeService = new EventTypeService(eventTypeRepository, timelineService, partitionResolver, enrichment,
                subscriptionDbRepository, schemaEvolutionService, partitionsCalculator, featureToggleService,
                authorizationValidator, timelineSync, transactionTemplate, nakadiSettings, nakadiKpiPublisher,
                KPI_ET_LOG_EVENT_TYPE, nakadiAuditLogPublisher, eventTypeOptionsValidator,
                adminService, mock(SubscriptionClientFactory.class), 1000);
        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            final TransactionCallback callback = (TransactionCallback) invocation.getArguments()[0];
            return callback.doInTransaction(null);
        });

    }

    @Test
    public void testFailToDeleteEventType() throws Exception {
        final EventType eventType = buildDefaultEventType();
        doThrow(new InternalNakadiException("Can't delete event tye"))
                .when(eventTypeRepository).removeEventType(eventType.getName());
        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        final Multimap<TopicRepository, String> topicsToDelete = mock(Multimap.class);
        doReturn(new ArrayList<Subscription>())
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 1);
        doReturn(topicsToDelete).when(timelineService).deleteAllTimelinesForEventType(eventType.getName());
        try {
            eventTypeService.delete(eventType.getName());
        } catch (final EventTypeDeletionException e) {
            // check that topics are not deleted in Kafka
            verifyZeroInteractions(topicsToDelete);
            return;
        }

        fail("Should have thrown an EventTypeDeletionException");
    }

    @Test(expected = ConflictException.class)
    public void whenSubscriptionsExistThenCantDeleteEventType() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doReturn(ImmutableList.of(RandomSubscriptionBuilder.builder().build()))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 1);

        eventTypeService.delete(eventType.getName());
    }

    @Test
    public void testFeatureToggleAllowsDeleteEventTypeWithSubscriptions() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doReturn(ImmutableList.of(RandomSubscriptionBuilder.builder().build()))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 1);

        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS))
                .thenReturn(true);

        eventTypeService.delete(eventType.getName());
        // no exception should be thrown
    }

    @Test
    public void testFeatureToggleAllowsDeletEventTypeWithAuthzSectionAndDeletableSubscription() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setAuthorization(buildResourceAuthorization());

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doReturn(ImmutableList.of(createSubscription("nakadi_archiver", "nakadi_to_s3")))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 20);
        doReturn(ImmutableList.of(createSubscription("nakadi_archiver", "nakadi_to_s3")))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 1);
        doReturn(Lists.emptyList())
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 20, 20);
        doReturn("nakadi_archiver").when(nakadiSettings).getDeletableSubscriptionOwningApplication();
        doReturn("nakadi_to_s3").when(nakadiSettings).getDeletableSubscriptionConsumerGroup();

        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.EVENT_TYPE_DELETION_ONLY_ADMINS))
                .thenReturn(true);

        eventTypeService.delete(eventType.getName());
        // no exception should be thrown
    }

    @Test
    public void testFeatureToggleForbidsDeleteEventTypeWithoutAuthzSection() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doReturn(ImmutableList.of(createSubscription("nakadi_archiver", "nakadi_to_s3")))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 20);
        doReturn(Lists.emptyList())
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 20, 20);
        doReturn("nakadi_archiver").when(nakadiSettings).getDeletableSubscriptionOwningApplication();
        doReturn("nakadi_to_s3").when(nakadiSettings).getDeletableSubscriptionConsumerGroup();

        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.EVENT_TYPE_DELETION_ONLY_ADMINS))
                .thenReturn(true);

        try {
            eventTypeService.delete(eventType.getName());
        } catch (AccessDeniedException e) {
            return;
        }
        fail("Should throw AccessDeniedException");
    }

    @Test
    public void testFeatureToggleForbidsDeleteEventTypeWithNonDeletableSubscription() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setAuthorization(buildResourceAuthorization());

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doReturn(ImmutableList.of(createSubscription("someone", "something")))
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 20);
        doReturn(Lists.emptyList())
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 20, 20);
        doReturn("nakadi_archiver").when(nakadiSettings).getDeletableSubscriptionOwningApplication();
        doReturn("nakadi_to_s3").when(nakadiSettings).getDeletableSubscriptionConsumerGroup();

        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.EVENT_TYPE_DELETION_ONLY_ADMINS))
                .thenReturn(true);

        try {
            eventTypeService.delete(eventType.getName());
        } catch (AccessDeniedException e) {
            return;
        }
        fail("Should throw AccessDeniedException");
    }

    @Test
    public void testAllowCreatingEventTypeWithInformationalFieldsFromEffectiveSchema() {
        final EventType et = EventTypeTestBuilder.builder()
                .category(EventCategory.DATA)
                .build();
        et.setOrderingKeyFields(Collections.singletonList("metadata.occurred_at"));
        et.setOrderingInstanceIds(Collections.singletonList("metadata.partition"));

        try {
            eventTypeService.create(et, true);
        } catch (final InvalidEventTypeException e) {
            fail("Cannot create event with informational fields from effective schema");
        }
    }

    @Test(expected = FeatureNotAvailableException.class)
    public void testFeatureToggleDisableLogCompaction() {
        final EventType eventType = buildDefaultEventType();
        eventType.setCleanupPolicy(CleanupPolicy.COMPACT);

        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_LOG_COMPACTION))
                .thenReturn(true);

        eventTypeService.create(eventType, true);
    }

    @Test
    public void shouldRemoveEventTypeWhenTimelineCreationFails() {
        final EventType eventType = buildDefaultEventType();
        when(timelineService.createDefaultTimeline(any(), anyInt()))
                .thenThrow(new TopicCreationException("Failed to create topic"));
        try {
            eventTypeService.create(eventType, true);
            fail("should throw TopicCreationException");
        } catch (final TopicCreationException e) {
            // expected
        }

        verify(eventTypeRepository, times(1)).removeEventType(eventType.getName());
    }

    @Test
    public void whenEventTypeCreatedThenKPIEventSubmitted() {
        final EventType et = buildDefaultEventType();
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
    public void whenEventTypeUpdatedThenKPIEventSubmitted() throws Exception {
        final EventType et = buildDefaultEventType();
        when(eventTypeRepository.findByName(et.getName())).thenReturn(et);
        when(schemaEvolutionService.evolve(any(), any())).thenReturn(et);
        when(nakadiSettings.getMaxTopicPartitionCount()).thenReturn(32);
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
    public void whenEventTypeDeletedThenKPIEventSubmitted() throws Exception {
        final EventType et = buildDefaultEventType();
        when(eventTypeRepository.findByNameO(et.getName())).thenReturn(Optional.of(et));

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
