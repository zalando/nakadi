package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.config.ValidatorConfig;
import org.zalando.nakadi.controller.advice.EventTypeExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaConfig;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.problem.Problem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.DISABLE_EVENT_TYPE_DELETION;
import static org.zalando.nakadi.util.PrincipalMockFactory.mockPrincipal;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventTypeControllerTestCase {

    protected static final long TOPIC_RETENTION_MIN_MS = 86400000;
    protected static final long TOPIC_RETENTION_MAX_MS = 345600000;
    protected static final long TOPIC_RETENTION_TIME_MS = 172800000;
    protected static final int NAKADI_SEND_TIMEOUT = 10000;
    protected static final int NAKADI_POLL_TIMEOUT = 10000;
    protected static final long NAKADI_EVENT_MAX_BYTES = 1000000;
    protected static final int NAKADI_SUBSCRIPTION_MAX_PARTITIONS = 8;
    protected final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
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
    protected final SchemaEvolutionService schemaEvolutionService = new ValidatorConfig()
            .schemaEvolutionService();
    protected final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    protected final AdminService adminService = mock(AdminService.class);
    protected final NakadiKpiPublisher nakadiKpiPublisher = mock(NakadiKpiPublisher.class);
    protected final NakadiAuditLogPublisher nakadiAuditLogPublisher = mock(NakadiAuditLogPublisher.class);

    protected MockMvc mockMvc;

    public EventTypeControllerTestCase() throws IOException {
    }

    @Before
    public void init() throws Exception {

        final NakadiSettings nakadiSettings = new NakadiSettings(0, 0, 0, TOPIC_RETENTION_TIME_MS, 0, 60,
                NAKADI_POLL_TIMEOUT, NAKADI_SEND_TIMEOUT, 0, NAKADI_EVENT_MAX_BYTES,
                NAKADI_SUBSCRIPTION_MAX_PARTITIONS, "service", "nakadi", "I am warning you",
                "I am warning you, even more");
        final PartitionsCalculator partitionsCalculator = new KafkaConfig().createPartitionsCalculator(
                "t2.large", TestUtils.OBJECT_MAPPER, nakadiSettings);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepository);
        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            final TransactionCallback callback = (TransactionCallback) invocation.getArguments()[0];
            return callback.doInTransaction(null);
        });

        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        final EventTypeService eventTypeService = new EventTypeService(eventTypeRepository, timelineService,
                partitionResolver, enrichment, subscriptionRepository, schemaEvolutionService, partitionsCalculator,
                featureToggleService, authorizationValidator, timelineSync, transactionTemplate, nakadiSettings,
                nakadiKpiPublisher, "et-log-event-type", nakadiAuditLogPublisher, eventTypeOptionsValidator,
                adminService);
        final EventTypeController controller = new EventTypeController(eventTypeService, featureToggleService,
                adminService, nakadiSettings);
        doReturn(randomUUID).when(uuid).randomUUID();

        doReturn(true).when(applicationService).exists(any());

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, featureToggleService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new EventTypeExceptionHandler())
                .build();
    }

    protected ResultActions deleteEventType(final String eventTypeName) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName));
    }

    protected ResultActions deleteEventType(final String eventTypeName, final String clientId) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName).principal(mockPrincipal(clientId)));
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

    protected ResultActions putEventType(final EventType eventType, final String name, final String clientId)
            throws Exception {
        final String content = TestUtils.OBJECT_MAPPER.writeValueAsString(eventType);

        return putEventType(content, name, clientId);
    }

    protected ResultActions putEventType(final EventType eventType, final String name) throws Exception {
        final String content = TestUtils.OBJECT_MAPPER.writeValueAsString(eventType);

        return putEventType(content, name);
    }

    protected ResultActions putEventType(final String content, final String name) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name).contentType(APPLICATION_JSON)
                .content(content);
        return mockMvc.perform(requestBuilder);
    }

    protected ResultActions putEventType(final String content, final String name, final String clientId)
            throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name)
                .principal(mockPrincipal(clientId))
                .contentType(APPLICATION_JSON)
                .content(content);
        return mockMvc.perform(requestBuilder);
    }

    protected ResultActions getEventType(final String eventTypeName) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + eventTypeName);
        return mockMvc.perform(requestBuilder);
    }

    protected SameJSONAs<? super String> matchesProblem(final Problem expectedProblem) throws JsonProcessingException {
        return sameJSONAs(TestUtils.OBJECT_MAPPER.writeValueAsString(expectedProblem));
    }

    protected void disableETDeletionFeature() {
        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(DISABLE_EVENT_TYPE_DELETION);
    }

}
