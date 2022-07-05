package org.zalando.nakadi.service;

import com.google.common.collect.Multimap;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidOwningApplicationException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.kpi.event.NakadiEventTypeLog;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

@RunWith(MockitoJUnitRunner.class)
public class EventTypeServiceTest {

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

    @Captor
    private ArgumentCaptor<Supplier<SpecificRecord>> kpiEventCaptor;

    @Before
    public void setUp() {
        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        eventTypeService = new EventTypeService(eventTypeRepository, timelineService, partitionResolver, enrichment,
                subscriptionDbRepository, schemaEvolutionService, partitionsCalculator, featureToggleService,
                authorizationValidator, timelineSync, transactionTemplate, nakadiSettings, nakadiKpiPublisher,
                nakadiAuditLogPublisher, eventTypeOptionsValidator,
                eventTypeCache, schemaService, adminService, applicationService);

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
    public void testFeatureToggleForbidsDeleteEventTypeWithoutAuthzSection() {
        final EventType eventType = TestUtils.buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeCache).getEventTypeIfExists(eventType.getName());

        when(featureToggleService.isFeatureEnabled(Feature.FORCE_EVENT_TYPE_AUTHZ))
                .thenReturn(true);

        assertThrows(AccessDeniedException.class, () -> eventTypeService.delete(eventType.getName()));
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
        verify(nakadiKpiPublisher).publish(kpiEventCaptor.capture());
        final NakadiEventTypeLog kpiEvent = (NakadiEventTypeLog) kpiEventCaptor.getValue().get();
        assertEquals(et.getName(), kpiEvent.getEventType());
        assertEquals("created", kpiEvent.getStatus());
        assertEquals(et.getCategory().toString(), kpiEvent.getCategory());
        assertEquals("disabled", kpiEvent.getAuthz());
        assertEquals(et.getCompatibilityMode().toString(), kpiEvent.getCompatibilityMode());
    }

    @Test
    public void whenEventTypeUpdatedThenKPIEventSubmitted() {
        final EventType et = TestUtils.buildDefaultEventType();
        when(eventTypeRepository.findByName(et.getName())).thenReturn(et);
        when(schemaEvolutionService.evolve(any(), any())).thenReturn(et);
        eventTypeService.update(et.getName(), et);
        verify(nakadiKpiPublisher).publish(kpiEventCaptor.capture());
        final NakadiEventTypeLog kpiEvent = (NakadiEventTypeLog) kpiEventCaptor.getValue().get();
        assertEquals(et.getName(), kpiEvent.getEventType());
        assertEquals("updated", kpiEvent.getStatus());
        assertEquals(et.getCategory().toString(), kpiEvent.getCategory());
        assertEquals("disabled", kpiEvent.getAuthz());
        assertEquals(et.getCompatibilityMode().toString(), kpiEvent.getCompatibilityMode());
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
        verify(nakadiKpiPublisher).publish(kpiEventCaptor.capture());
        final NakadiEventTypeLog kpiEvent = (NakadiEventTypeLog) kpiEventCaptor.getValue().get();
        assertEquals(et.getName(), kpiEvent.getEventType());
        assertEquals("deleted", kpiEvent.getStatus());
        assertEquals(et.getCategory().toString(), kpiEvent.getCategory());
        assertEquals("disabled", kpiEvent.getAuthz());
        assertEquals(et.getCompatibilityMode().toString(), kpiEvent.getCompatibilityMode());
    }

    @Test
    public void whenMetadataEventOwnerSelectorThenValueUnset() {
        final EventType et = TestUtils.buildDefaultEventType();

        et.setEventOwnerSelector(new EventOwnerSelector(EventOwnerSelector.Type.METADATA, "any_name", null));
        assertDoesNotThrow(() -> EventTypeService.validateEventOwnerSelector(et));

        et.setEventOwnerSelector(new EventOwnerSelector(EventOwnerSelector.Type.METADATA, "other_name", "some_value"));
        assertThrows(InvalidEventTypeException.class, () -> EventTypeService.validateEventOwnerSelector(et));
    }
}
