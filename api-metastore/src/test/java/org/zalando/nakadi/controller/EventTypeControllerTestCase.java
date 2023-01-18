package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SchemaValidatorConfig;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.EventTypeExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaConfig;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.AvroSchemaCompatibility;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.SchemaEvolutionService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.schema.CompatibilityModeChangeConstraint;
import org.zalando.nakadi.validation.schema.PartitionStrategyConstraint;
import org.zalando.problem.Problem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.domain.Feature.DISABLE_EVENT_TYPE_DELETION;

public class EventTypeControllerTestCase {

    protected static final long TOPIC_RETENTION_MIN_MS = 10800000;
    protected static final long TOPIC_RETENTION_MAX_MS = 345600000;
    protected static final long TOPIC_RETENTION_TIME_MS = 172800000;
    protected static final int NAKADI_SEND_TIMEOUT = 10000;
    protected static final int NAKADI_POLL_TIMEOUT = 10000;
    protected static final long NAKADI_EVENT_MAX_BYTES = 1000000;
    protected static final int NAKADI_SUBSCRIPTION_MAX_PARTITIONS = 8;
    protected final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    protected final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
    protected final TopicRepository topicRepository = mock(TopicRepository.class);
    protected final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    protected final Enrichment enrichment = mock(Enrichment.class);
    protected final UUIDGenerator uuid = mock(UUIDGenerator.class);
    protected final UUID randomUUID = new UUIDGenerator().randomUUID();
    protected final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    protected final SecuritySettings settings = mock(SecuritySettings.class);
    protected final ApplicationService applicationService = mock(ApplicationService.class);
    protected final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
    protected final TimelineService timelineService = mock(TimelineService.class);
    protected final TimelineSync timelineSync = mock(TimelineSync.class);
    protected final TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);
    protected final AdminService adminService = mock(AdminService.class);
    protected final AuthorizationService authorizationService = mock(AuthorizationService.class);
    protected final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    protected final NakadiKpiPublisher nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
    protected final NakadiAuditLogPublisher nakadiAuditLogPublisher = mock(NakadiAuditLogPublisher.class);
    private final SchemaService schemaService = mock(SchemaService.class);
    private final AvroSchemaCompatibility avroSchemaCompatibility = Mockito.mock(AvroSchemaCompatibility.class);
    protected MockMvc mockMvc;

    public EventTypeControllerTestCase() {
    }

    @Before
    public void init() throws Exception {

        final NakadiSettings nakadiSettings = new NakadiSettings(32, 0, 0, TOPIC_RETENTION_TIME_MS, 0, 60,
                NAKADI_POLL_TIMEOUT, NAKADI_SEND_TIMEOUT, 0, NAKADI_EVENT_MAX_BYTES,
                NAKADI_SUBSCRIPTION_MAX_PARTITIONS, "service", "org/zalando/nakadi", "I am warning you",
                "I am warning you, even more", "nakadi_archiver", "nakadi_to_s3", 100, 10000);
        final PartitionsCalculator partitionsCalculator = new KafkaConfig().createPartitionsCalculator(nakadiSettings);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepository);
        when(authorizationService.getSubject()).thenReturn(Optional.empty());
        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            final TransactionCallback callback = (TransactionCallback) invocation.getArguments()[0];
            return callback.doInTransaction(null);
        });
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(false);

        final SchemaEvolutionService ses = new SchemaValidatorConfig(
                new CompatibilityModeChangeConstraint(adminService, authorizationService),
                new PartitionStrategyConstraint(adminService),
                avroSchemaCompatibility
        ).schemaEvolutionService();

        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        final EventTypeService eventTypeService = new EventTypeService(eventTypeRepository, timelineService,
                partitionResolver, enrichment, subscriptionRepository, ses, partitionsCalculator,
                featureToggleService, authorizationValidator, timelineSync, transactionTemplate, nakadiSettings,
                nakadiKpiPublisher, nakadiAuditLogPublisher,
                eventTypeOptionsValidator, eventTypeCache,
                schemaService, adminService, null);

        final EventTypeController controller = new EventTypeController(eventTypeService, featureToggleService,
                adminService, nakadiSettings);
        doReturn(randomUUID).when(uuid).randomUUID();

        doReturn(true).when(applicationService).exists(any());

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new EventTypeExceptionHandler())
                .build();
    }

    protected ResultActions deleteEventType(final String eventTypeName) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName));
    }

    protected ResultActions postEventType(final EventType eventType) throws Exception {
        final String content = TestUtils.OBJECT_MAPPER.writeValueAsString(eventType);

        return postEventType(content);
    }

    protected ResultActions postEventType(final String content) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/event-types").contentType(APPLICATION_JSON).content(
                content);

        return mockMvc.perform(requestBuilder);
    }

    protected ResultActions putEventType(final EventType eventType, final String name) throws Exception {
        final String content = TestUtils.OBJECT_MAPPER.writeValueAsString(eventType);

        return putEventType(content, name);
    }

    protected ResultActions putEventType(final String content, final String name) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name)
                .contentType(APPLICATION_JSON)
                .content(content);
        return mockMvc.perform(requestBuilder);
    }

    protected ResultActions getEventType(final String eventTypeName) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + eventTypeName);
        return mockMvc.perform(requestBuilder);
    }

    protected ResultActions getEventTypes(final String writer, final String owningApp) throws Exception {
        final List<String> query = Lists.newArrayList();
        if (writer != null && !writer.isEmpty()) {
            query.add("writer=" + writer);
        }
        if (owningApp != null && !owningApp.isEmpty()) {
            query.add("owning_application=" + owningApp);
        }
        final MockHttpServletRequestBuilder requestBuilder = get("/event-types?" + String.join("&", query));
        return mockMvc.perform(requestBuilder);
    }

    protected SameJSONAs<? super String> matchesProblem(final Problem expectedProblem) throws JsonProcessingException {
        return SameJSONAs.sameJSONAs(TestUtils.OBJECT_MAPPER.writeValueAsString(expectedProblem));
    }

    protected void disableETDeletionFeature() {
        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(DISABLE_EVENT_TYPE_DELETION);
    }

}
